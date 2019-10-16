package org.springframework.data.mongodb.gridfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.io.IOException;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StreamUtils;
import reactor.test.StepVerifier;

/**
 * Integration tests for {@link ReactiveGridFsTemplate} in combination with the deprecated GridFs.
 *
 * @author Nick Stolwijk
 */
@RunWith(SpringRunner.class)
@ContextConfiguration({"classpath:gridfs/reactive-gridfs.xml"})
public class ReactiveGridFsTemplateWithOldGridFsTests {

    Resource resource = new ClassPathResource("gridfs/gridfs.xml");

    @Autowired ReactiveGridFsOperations operations;

    @Autowired SimpleMongoDbFactory mongoClient;
    @Before
    public void setUp() {

        operations.delete(new Query()) //
                .as(StepVerifier::create) //
                .verifyComplete();
    }

    @Test // DATAMONGO-2392
    public void storeFileWithOldGridFsAndFindItWithReactiveGridFsOperations() throws IOException {
        byte[] content = StreamUtils.copyToByteArray(resource.getInputStream());
        String reference = "1af52e25-76b7-4e34-8618-9baa183c9112";

        GridFS fs = new GridFS(mongoClient.getLegacyDb());
        GridFSInputFile in = fs.createFile(resource.getInputStream(), "gridfs.xml");

        in.put("_id", reference);
        in.put("contentType", "application/octet-stream");
        in.save();

        operations.findOne(query(where("_id").is(reference))).flatMap(operations::getResource)
                .flatMapMany(ReactiveGridFsResource::getDownloadStream) //
                .transform(DataBufferUtils::join) //
                .as(StepVerifier::create) //
                .consumeNextWith(dataBuffer -> {

                    byte[] actual = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(actual);

                    assertThat(actual).isEqualTo(content);
                }) //
                .verifyComplete();
    }
}
