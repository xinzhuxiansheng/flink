package com.yzhou.blog.wordcount;

import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.MetadataSerializer;
import org.apache.flink.runtime.checkpoint.metadata.MetadataSerializers;
import org.apache.flink.state.api.runtime.SavepointLoader;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import static org.apache.flink.runtime.checkpoint.Checkpoints.HEADER_MAGIC_NUMBER;

public class DiyReadCheckpoint {

    public static void main(String[] args) {
        String metadataPath = "D:\\TMP\\c43c2293d311ddc1b6151451bcc95a71\\chk-7\\_metadata";

        try (DataInputStream dis = new DataInputStream(new FileInputStream(metadataPath))) {

            final int magicNumber = dis.readInt();

            if (magicNumber == HEADER_MAGIC_NUMBER) {
                final int version = dis.readInt();

                long checkpointId = dis.readLong();

                int numMasterStates = dis.readInt();
                final int numTaskStates = dis.readInt();



                final MetadataSerializer serializer = MetadataSerializers.getSerializer(version);

            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
