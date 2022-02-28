package com.rktirtho.resources.service;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class S3Service {


    public boolean uploadFile(String bucketName, String s3ObjectKey, MultipartFile file, String fileName) {
        File multiPartFile = convertMultiPartToFile(file, fileName);
        getAmazonS3Connection().putObject(new PutObjectRequest(bucketName, s3ObjectKey, multiPartFile));
        return multiPartFile.delete();
    }

    public Map<InputStream, Long> downloadFile( String fileName, String location){
        String fileLocation = location.concat("/").concat(fileName);
        S3Object s3Object = getAmazonS3Connection().getObject("CONTENT_BUCKET", fileLocation);
        InputStream inputStream = s3Object.getObjectContent();
        Map<InputStream, Long> file = new HashMap<>();
        file.put(inputStream, s3Object.getObjectMetadata().getContentLength());

        return file;
    }

    private File convertMultiPartToFile(MultipartFile file, String fileName) {
        File convertFile = new File(fileName);

        try (FileOutputStream fileOutputStream = new FileOutputStream(convertFile)) {
            fileOutputStream.write(file.getBytes());
        } catch (IOException e) {
            log.error("Error to convert file", e);
        }
        return convertFile;
    }


    private AmazonS3 getAmazonS3Connection() {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials("ACCESS_KEY", "SECRETE_KEY");
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion("s3_REGION")
                .build();
    }
}
