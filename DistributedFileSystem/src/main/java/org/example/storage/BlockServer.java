package org.example.storage;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BlockServer {

    private static final String SERVER_STORAGE_DIR= "server_storage/chunks/";
    private Server server;

    private void start() throws IOException {
        File dir= new File(SERVER_STORAGE_DIR);

        if(!dir.exists()){
            dir.mkdirs();
        }
        int port= 50051;
        server= ServerBuilder.forPort(port)
                .maxInboundMessageSize(10 * 1024 * 1024)
                .addService(new BlockStorageImpl())
                .build()
                .start();

        System.out.println("Server started, listening on port: "+port);


        //ADDED SHUTDOWN HOOK TO RUN WHEN JVM SHUTTING DOWN
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.err.println("Shutting down server on port: "+port);

            if(server!=null){
                server.shutdown();
            }
        }));

    }

    private void BlockUntilShutdown() throws InterruptedException {
        if(server!=null){
            server.awaitTermination();
        }
    }

    static void main() throws IOException, InterruptedException {
        final BlockServer blockServer = new BlockServer();
        blockServer.start();
        blockServer.BlockUntilShutdown();
    }

    static class BlockStorageImpl extends BlockStorageGrpc.BlockStorageImplBase{
        @Override
        public void fetchChunk(HashRequest request, StreamObserver<ChunkResponse> responseObserver) {
            String hash= request.getHashId();

            File storedFile= new File(SERVER_STORAGE_DIR+hash+".chunk");

            try(RandomAccessFile raf= new RandomAccessFile(storedFile,"r")){
                FileChannel inChannel = raf.getChannel();

                //Since our chunk can be of maximum 4MB , we can safely cast the long type
                //channel size to int type
                ByteBuffer byteBuffer= ByteBuffer.allocate((int) inChannel.size());

                inChannel.read(byteBuffer);
                byteBuffer.flip();

                ByteString  protobufByte= ByteString.copyFrom(byteBuffer);

                ChunkResponse chunkResponse= ChunkResponse.newBuilder()
                        .setSuccess(true)
                        .setData(protobufByte)
                        .build();

                responseObserver.onNext(chunkResponse);
                responseObserver.onCompleted();


            } catch (IOException e) {
                System.err.println("Unable to fetch File");

                ChunkResponse chunkResponse= ChunkResponse.newBuilder().setSuccess(false).build();

                responseObserver.onNext(chunkResponse);
                responseObserver.onCompleted();
            }
        }

        @Override
        public void uploadChunk(ChunkRequest request, StreamObserver<UploadResponse> responseObserver) {
            String hash= request.getHashId();
            ByteString bytes= request.getData();
            ByteBuffer byteBuffer= bytes.asReadOnlyByteBuffer();

            File chunkFile= new File(SERVER_STORAGE_DIR+hash+".chunk");

            try(FileOutputStream fos= new FileOutputStream(chunkFile)){
                FileChannel outputChannel= fos.getChannel();

                outputChannel.write(byteBuffer);

                UploadResponse response= UploadResponse.newBuilder()
                        .setSuccess(true)
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();

            }
            catch (Exception e){
                System.err.println("Unable to upload File"+e.getMessage());
                UploadResponse response= UploadResponse.newBuilder()
                        .setSuccess(false)
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();

                e.printStackTrace();
            }


        }

        @Override
        public void checkChunkExists(HashRequest request, StreamObserver<ExistResponse> responseObserver) {
            String hash= request.getHashId();
            File chunkFile= new File(SERVER_STORAGE_DIR+hash+".chunk");

            ExistResponse response= ExistResponse.newBuilder()
                    .setExists(chunkFile.exists())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
