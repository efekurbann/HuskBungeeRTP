package me.william278.huskbungeertp.jedis;

import me.william278.huskbungeertp.HuskBungeeRTP;
import me.william278.huskbungeertp.HuskHomesExecutor;
import me.william278.huskbungeertp.MessageManager;
import me.william278.huskbungeertp.mysql.DataHandler;
import me.william278.huskbungeertp.randomtp.RtpHandler;
import me.william278.huskbungeertp.randomtp.processor.AbstractRtp;
import me.william278.huskhomes2.teleport.points.TeleportationPoint;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Player;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.UUID;

public class RedisMessenger {

    private static final String REDIS_CHANNEL = "HuskBungeeRtp";
    private static JedisPool jedisPool;

    public static void subscribe() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(16);
        jedisPool = new JedisPool(config, HuskBungeeRTP.getSettings().getRedisHost(),
                HuskBungeeRTP.getSettings().getRedisPort(), 2000, HuskBungeeRTP.getSettings().getRedisPassword());

        try (Jedis jedis = jedisPool.getResource()) {
            new Thread(() -> jedis.subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    if (!channel.equals(REDIS_CHANNEL)) {
                        return;
                    }
                    String[] splitMessage = message.split("£");
                    RedisMessage receivedMessage = new RedisMessage(splitMessage[0], RedisMessage.RedisMessageType.valueOf(splitMessage[1]), splitMessage[2]);
                    if (!receivedMessage.getTargetServer().equals(HuskBungeeRTP.getSettings().getServerId())) {
                        return;
                    }
                    Bukkit.getScheduler().runTask(HuskBungeeRTP.getInstance(), () -> handleReceivedMessage(receivedMessage));
                }
            }, REDIS_CHANNEL), "Redis Subscriber").start();
        }
    }

    public static void publish(RedisMessage message) {
        Bukkit.getScheduler().runTaskAsynchronously(HuskBungeeRTP.getInstance(), () -> {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.publish(REDIS_CHANNEL, message.toString());
            }
        });
    }

    public static void handleReceivedMessage(RedisMessage message) {
        String[] messageData = message.getMessageData().split("#");
        switch (message.getMessageType()) {
            case REPLY_RANDOM_FAILED -> {
                final UUID originPlayerUUID = UUID.fromString(messageData[0]);
                final int attemptsTaken = Integer.parseInt(messageData[1]);
                Player player = Bukkit.getPlayer(originPlayerUUID);
                if (player != null) {
                    MessageManager.sendMessage(player, "error_rtp_failed", Integer.toString(attemptsTaken));
                }
                RtpHandler.removeRtpUser(originPlayerUUID);
            }
            case REQUEST_RANDOM_LOCATION -> {
                // An incoming request that this server find and return a random location
                final UUID sourcePlayerUUID = UUID.fromString(messageData[0]);
                final String sourceServer = messageData[1];
                final String targetWorld = messageData[2];
                final String targetBiomeString = messageData[3];
                final String targetGroupId = messageData[4];
                World world = Bukkit.getWorld(targetWorld);
                if (world != null) {
                    AbstractRtp.RandomResult result = HuskBungeeRTP.getAbstractRtp().getRandomLocation(world, targetBiomeString);
                    if (!result.successful()) {
                        publish(new RedisMessage(sourceServer, RedisMessage.RedisMessageType.REPLY_RANDOM_FAILED,
                                sourcePlayerUUID + "#" + result.attemptsTaken()));
                        return;
                    }
                    final Location randomLocation = result.location();
                    world.getChunkAt(randomLocation); // Load the chunk
                    publish(new RedisMessage(sourceServer, RedisMessage.RedisMessageType.REPLY_RANDOM_LOCATION,
                            sourcePlayerUUID + "#" + HuskBungeeRTP.getSettings().getServerId() + "#" +
                                    world.getName() + "#" + randomLocation.getX() + "#" +
                                    randomLocation.getY() + "#" + randomLocation.getZ() + "#" + targetGroupId));
                }
            }
            case REPLY_RANDOM_LOCATION -> {
                // A reply from a server that a location has been found and a player should be teleported
                final UUID originPlayerUUID = UUID.fromString(messageData[0]);
                final String sourceServer = messageData[1];
                final String locationWorld = messageData[2];
                final double locationX = Double.parseDouble(messageData[3]);
                final double locationY = Double.parseDouble(messageData[4]);
                final double locationZ = Double.parseDouble(messageData[5]);
                final String destinationGroupId = messageData[6];

                Player player = Bukkit.getPlayer(originPlayerUUID);
                if (player != null) {
                    HuskHomesExecutor.teleportPlayer(player, new TeleportationPoint(
                            locationWorld, locationX, locationY, locationZ, 0F, 0F, sourceServer));

                    // Apply cool down
                    if (!player.hasPermission("huskrtp.bypass_cooldown")) {
                        DataHandler.setPlayerOnCoolDown(originPlayerUUID, HuskBungeeRTP.getSettings().getGroupById(destinationGroupId),
                                new TeleportationPoint(locationWorld, locationX, locationY, locationZ, 0F, 0F, sourceServer));
                    }
                }
                RtpHandler.removeRtpUser(originPlayerUUID);

            }
            case GET_PLAYER_COUNT -> {
                final String sourceServer = messageData[0];
                //final long requestTime = Long.parseLong(messageData[1]);

                publish(new RedisMessage(sourceServer, RedisMessage.RedisMessageType.RETURN_PLAYER_COUNT,
                        HuskBungeeRTP.getSettings().getServerId() + "#" + Bukkit.getOnlinePlayers().size()));
            }
            case RETURN_PLAYER_COUNT -> {
                final String sourceServer = messageData[0];
                final int sourcePlayerCount = Integer.parseInt(messageData[1]);

                HuskBungeeRTP.serverPlayerCounts.put(sourceServer, sourcePlayerCount);
            }
        }
    }

}