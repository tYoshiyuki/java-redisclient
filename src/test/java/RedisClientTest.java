import lombok.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisClientTest {
    private List<SampleEntity> input = new ArrayList<>();
    private RedisClient client;

    @Before
    public void setUp() {
        client = new RedisClient(URI.create("http://localhost:6379"));
    }

    @Test
    public void setAndGetEntity_正常系() throws IOException {
        // Arrange
        SampleEntity expect = SampleEntity.builder()
                .id(1)
                .name("Taro Yamada")
                .build();

        // Act
        client.setEntity(String.valueOf(expect.getId()), expect);

        // Assert
        SampleEntity result = client.getEntity(SampleEntity.class, String.valueOf(expect.getId()));
        assertThat(result.getId()).isEqualTo(expect.getId());
        assertThat(result.getName()).isEqualTo(expect.getName());
    }

    @Test
    public void beginAndReleaseLock_正常系() throws IOException {
        // Arrange
        SampleEntity input = SampleEntity.builder()
                .id(2)
                .name("Jiro Tanaka")
                .build();
        client.releaseLock(String.valueOf(input.getId()));

        // Act
        boolean result1 = client.beginLock(String.valueOf(input.getId()));
        boolean result2 = client.beginLock(String.valueOf(input.getId())); // ロック中のためロック取得に失敗するはず
        client.releaseLock(String.valueOf(input.getId()));
        String result3 = client.get("lock:" + input.getId());

        // Assert
        assertThat(result1).isEqualTo(true);
        assertThat(result2).isEqualTo(false);
        assertThat(result3).isNullOrEmpty();
    }

    @Setter
    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SampleEntity {
        private int id;
        private String name;
    }
}