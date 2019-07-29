package redisclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.net.URI;

public class RedisClient {
    protected final Logger logger = LogManager.getLogger(this.getClass());
    protected JedisPool pool;

    private final int expireMsecs = 60 * 1000; // ロックの有効期限 (ミリ秒)
    private final int timeoutMsecs = 10 * 1000; // ロック取得時のタイムアウト時間 (ミリ秒)
    private final String lockPrefix = "lock:"; // ロック取得時のキープレフィックス

    public RedisClient(URI uri) {
        pool = new JedisPool(uri);
    }

    /**
     * 値を設定します
     * @param key
     * @param value
     */
    public void set(String key, String value) {
        Jedis jedis = pool.getResource();
        jedis.set(key, value);
        jedis.close();
    }

    /**
     * キーを条件に値を取得します
     * @param key
     * @return
     */
    public String get(String key) {
        Jedis jedis = pool.getResource();
        String value = jedis.get(key);
        jedis.close();
        return value;
    }

    /**
     * キーを条件に排他ロックを取得します
     * @param key
     * @return ロック取得に成功した場合は true, そうでない場合は false
     */
    public boolean beginLock(String key) {
        Jedis jedis = pool.getResource();

        try {
            return acquireLock(jedis, key);
        } catch (InterruptedException ex) {
            logger.error(ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * エンティティをJSON形式に変換し設定します
     * @param key
     * @param value
     * @throws JsonProcessingException
     */
    public <T> void setEntity(String key, T value) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(value);
        set(key, json);
    }

    /**
     * キーを条件にJSONを取得し、エンティティへバインドした結果を取得します
     * @param targetClass
     * @param key
     * @return
     * @throws IOException
     */
    public <T> T getEntity(Class<T> targetClass, String key) throws IOException {
        String value = get(key);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(value, targetClass);
    }

    /**
     * キーを条件に排他ロックを解放します
     * @param key
     */
    public void releaseLock(String key) {
        Jedis jedis = pool.getResource();
        release(jedis, key);
    }

    private synchronized void release(Jedis jedis, String lockKey) {
        jedis.del(lockPrefix + lockKey);
    }

    /**
     * キーを条件に排他ロックを取得します
     * @return ロック取得に成功した場合は true, そうでない場合は false
     * @throws InterruptedException
     */
    private synchronized boolean acquireLock(Jedis jedis, String lockKey) throws InterruptedException {
        int timeout = timeoutMsecs;
        String key = lockPrefix + lockKey;
        while (timeout >= 0) {
            long expires = System.currentTimeMillis() + expireMsecs + 1;
            String expiresStr = String.valueOf(expires);

            if (jedis.setnx(key, expiresStr) == 1) {
                return true; // ロック取得成功
            }

            String currentValueStr = jedis.get(key);
            if (currentValueStr != null && Long.parseLong(currentValueStr) < System.currentTimeMillis()) {
                // ロックの有効期限が切れている場合の処理
                String oldValueStr = jedis.getSet(key, expiresStr);
                if (oldValueStr != null && oldValueStr.equals(currentValueStr)) {
                    return true; // ロック取得成功
                }
            }

            logger.warn("ロック取得に失敗したためリトライします。");
            timeout -= 1000;
            Thread.sleep(1000);
        }

        return false;
    }
}
