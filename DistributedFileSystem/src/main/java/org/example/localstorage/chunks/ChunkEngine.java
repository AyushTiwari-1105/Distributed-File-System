package org.example.localstorage.chunks;

import com.google.api.Http;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.example.storage.*;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ChunkEngine {
    private static final int CHUNK_SIZE= 4*1024*1024;

    private final ManagedChannel channel;
    private final BlockStorageGrpc.BlockStorageBlockingStub blockingStub;

    public ChunkEngine(){
        //The channel
        this.channel= ManagedChannelBuilder.forAddress("localhost",50051)
                .usePlaintext()
                .maxInboundMessageSize(10*1024*1024)
                .build();

        this.blockingStub= BlockStorageGrpc.newBlockingStub(channel);
    }

    public void sliceAndUploadFile(String filePath){
        File sourceFile= new File(filePath);

        if(!sourceFile.exists()){
            System.out.println("File not Found");
            return;
        }

        List<String> chunkHashes= new ArrayList<>();

        try(RandomAccessFile raf= new RandomAccessFile(sourceFile,"r")){
            FileChannel fileChannel= raf.getChannel();

            ByteBuffer buffer= ByteBuffer.allocate(CHUNK_SIZE); // Fixed buffer of CHUNK_SIZE , used for reading and hashing every chunk

            int chunkIndex=0;

            MessageDigest digest= MessageDigest.getInstance("SHA-256");

            while(fileChannel.read(buffer)>0){

                buffer.flip();// Switch from write-to-buffer mode to read-from-buffer

                digest.update(buffer); //updates the position of bytebuffer to end, need to rewind to start for next block
                byte[] hash= digest.digest();
                String chunkHash= bytesToHex(hash);

                chunkHashes.add(chunkHash);

                //Sets position to 0 to start reading next block
                buffer.rewind();
                int bytesRead= buffer.limit();

                System.out.println("Processed Chunk "+ chunkIndex+" | Hash: "+ chunkHash+ " | Size: "+bytesRead);


                HashRequest hashRequest= HashRequest.newBuilder().setHashId(chunkHash).build();
                ExistResponse existResponse= blockingStub.checkChunkExists(hashRequest);

                if(existResponse.getExists()){
                    System.out.println("Server already has the uploaded chunk");
                }
                else{
                    ByteString protobufBytes= ByteString.copyFrom(buffer);

                    ChunkRequest chunkRequest= ChunkRequest.newBuilder().setHashId(chunkHash).setData(protobufBytes).build();

                    UploadResponse uploadResponse= blockingStub.uploadChunk(chunkRequest);

                    if(uploadResponse.getSuccess()){
                        System.out.println("Upload Successful");
                    }
                    else{
                        System.err.println("Upload Failed");
                    }
                }

                buffer.clear();
                chunkIndex++;
            }


            System.out.println("Total chunks processed: "+ chunkIndex);

            System.out.println("Sending metadata to control plane");

            String hashJsonArray= chunkHashes.stream()
                    .map(hash->"\"" + hash + "\"")
                    .collect(Collectors.joining(","));

            String JsonPayload= String.format(
                    "{\"fileName\":\"%s\", \"chunkCount\":\"%d\", \"chunkHashes\":[%s]}"
                    ,sourceFile.getName(),chunkIndex,hashJsonArray
            );


            try(HttpClient httpClient=HttpClient.newHttpClient()) {

                HttpRequest httpRequest = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8080/api/metadata"))
                        .header("Content-Type","application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(JsonPayload))
                        .build();
                System.out.println(JsonPayload);

                HttpResponse<String> httpResponse= httpClient.send(httpRequest,HttpResponse.BodyHandlers.ofString());

                if(httpResponse.statusCode()==200){
                    System.out.println("Successfully added index file to control plane");
                }
                else{
                    System.err.println("Unable to add index file to control plane");
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }


        } catch (Exception e) {
            System.err.println("Error processing file: "+ e.getMessage());

        }

    }


    //Converts Raw bytes to HexString
    private String bytesToHex(byte[] bytes) {
            StringBuilder hexString= new StringBuilder();
            for(byte b:bytes){
                hexString.append(String.format("%02x",b));
            }
            return hexString.toString();
    }

    public void shutdown(){
        if(channel!=null){
            channel.shutdown();
        }
    }

    static void main(String[] args) {
        ChunkEngine chunkEngine= new ChunkEngine();

        String testFilePath= "src/main/resources/Book_DigitalImageProcessing.pdf";
        System.out.println("Starting file slice engine...");
        long startTime = System.currentTimeMillis();

        chunkEngine.sliceAndUploadFile(testFilePath);

        long endTime = System.currentTimeMillis();
        System.out.println("Completed in " + (endTime - startTime) + " ms");

        chunkEngine.shutdown();
    }
}
