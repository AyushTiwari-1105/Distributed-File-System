package org.example.metadata_server.controller;


import org.example.metadata_server.model.FileMetaData;
import org.example.metadata_server.repository.MetadataRepository;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/api/metadata")
public class MetadataController {
    private final MetadataRepository repository;

    public MetadataController (MetadataRepository repository){
        this.repository=repository;
    }


    @PostMapping
    public ResponseEntity<String> saveMetaData(@RequestBody FileMetaData metaData){

        int receivedCount= metaData.getChunkHashes().size();
        int actualCount= metaData.getChunkCount();

        if(receivedCount!=actualCount){
            System.err.println("Actual hash count does not match received hash count");
            return ResponseEntity.status(HttpStatusCode.valueOf(500)).build();
        }
        repository.save(metaData);
        return ResponseEntity.ok("Successfully saved meta data for: "+ metaData.getFileName());
    }

    @GetMapping("/{fileName}")
    public ResponseEntity<FileMetaData> getMetaData(@PathVariable String fileName ){
        Optional<FileMetaData> metaData= repository.findById(fileName);

        if(metaData.isPresent()){
            return ResponseEntity.ok(metaData.get());
        }
        else{
             return ResponseEntity.notFound().build();
        }
    }


}
