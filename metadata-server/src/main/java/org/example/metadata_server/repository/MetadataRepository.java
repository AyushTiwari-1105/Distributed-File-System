package org.example.metadata_server.repository;

import org.example.metadata_server.model.FileMetaData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MetadataRepository  extends JpaRepository<FileMetaData,String> {

}
