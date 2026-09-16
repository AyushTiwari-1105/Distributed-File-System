package org.example.localstorage.chunks;


import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.example.storage.BlockStorageGrpc;
import org.example.storage.ChunkResponse;
import org.example.storage.HashRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;

public class FileReconstructor {

    private final ManagedChannel channel;
    private final BlockStorageGrpc.BlockStorageBlockingStub blockingStub;

    public FileReconstructor(){
        this.channel= ManagedChannelBuilder.forAddress("localhost",50051)
                .usePlaintext()
                .maxInboundMessageSize(10*1024*1024)
                .build();

        this.blockingStub= BlockStorageGrpc.newBlockingStub(channel);
    }
    public void downloadAndReconstruct(String fileName) throws IOException {

        System.out.println("Fetching metaData from control plane");

        String jsonResponse;

        try (HttpClient httpClient = HttpClient.newHttpClient()) {
            HttpRequest httpRequest= HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:8080/api/metadata/"+fileName))
                    .GET()
                    .build();

            HttpResponse<String> httpResponse= httpClient.send(httpRequest,HttpResponse.BodyHandlers.ofString());


            if(httpResponse.statusCode()!=200){
                throw new InterruptedException();
            }
            jsonResponse= httpResponse.body();

            String hashArray= jsonResponse.substring(jsonResponse.indexOf("[")+1,jsonResponse.indexOf("]"));

            String cleanArray= hashArray.replace("\"","").replace(" ","");

            List<String> chunkHashes= Arrays.asList(cleanArray.split(","));

            int count= chunkHashes.size();


            System.out.println("Meta Data Received ->"+ jsonResponse);
            File restoredFile= new File("local_storage/network_restored_"+fileName);

            if(!restoredFile.getParentFile().exists()){
                restoredFile.getParentFile().mkdirs();
            }

            System.out.println("Downloading "+fileName+" from server...");

            try(FileOutputStream fos= new FileOutputStream(restoredFile)){
                FileChannel outChannel= fos.getChannel();

                for(int i=0;i<count;i++){
                    String hash= chunkHashes.get(i);

                    HashRequest hashRequest= HashRequest.newBuilder()
                            .setHashId(hash)
                            .build();

                    ChunkResponse chunkResponse= blockingStub.fetchChunk(hashRequest);

                    if(!chunkResponse.getSuccess()){
                        System.err.println("Server missing chunk with hash: "+hash);
                    }

                    ByteString dataFromResponse= chunkResponse.getData();
                    ByteBuffer dataInBuffer= dataFromResponse.asReadOnlyByteBuffer();

                    outChannel.write(dataInBuffer);
                }

            }
            catch (Exception e){
                System.err.println("Cannot Reconstruct File");
                e.printStackTrace();
            }

        } catch (InterruptedException e) {
            System.err.println("Unable to fetch metaData from control plane");
        }




    }

    public void shutdown(){
        if(channel!=null){
            channel.shutdown();
        }
    }

    static void main() {
        FileReconstructor fileReconstructor= new FileReconstructor();

        long startTime= System.currentTimeMillis();
        try {
            fileReconstructor.downloadAndReconstruct("Book_DigitalImageProcessing.pdf");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long endTime= System.currentTimeMillis();

        long timeTaken= endTime-startTime;

        System.out.println("The File was reconstructed in: "+timeTaken+" ms");

        fileReconstructor.shutdown();
    }
}
