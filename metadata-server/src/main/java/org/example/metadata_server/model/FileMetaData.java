package org.example.metadata_server.model;

import jakarta.persistence.*;

import java.util.List;

@Entity
@Table(name = "file_metadata")
public class FileMetaData {

    @Id
    private String fileName;

    private int chunkCount;


    @ElementCollection
    @CollectionTable(name = "chunk_hashes", joinColumns = @JoinColumn(name = "file_name"))
    @Column(name = "hash")
    private List<String> chunkHashes;

    public FileMetaData(){}


    public FileMetaData(String fileName, int chunkCount, List<String> chunkHashes) {
        this.fileName = fileName;
        this.chunkCount = chunkCount;
        this.chunkHashes = chunkHashes;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public String getFileName() {
        return fileName;
    }

    public List<String> getChunkHashes() {
        return chunkHashes;
    }
}
