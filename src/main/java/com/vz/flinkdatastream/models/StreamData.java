package com.vz.flinkdatastream.models;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.ZonedDateTime;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamData {
    private String email;
    String startDate;
    private String deptCode;
    private String location;
    private Long riderNum;
    private String status;

    public StreamData(String email, String startDate, String deptCode, String location, Long riderNum, String status) {
        this.email = email;
        this.startDate = startDate;
        this.deptCode = deptCode;
        this.location = location;
        this.riderNum = riderNum;
        this.status = status;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getDeptCode() {
        return deptCode;
    }

    public void setDeptCode(String deptCode) {
        this.deptCode = deptCode;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Long getRiderNum() {
        return riderNum;
    }

    public void setRiderNum(Long riderNum) {
        this.riderNum = riderNum;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "StreamData{" +
                "email='" + email + '\'' +
                ", startDate=" + startDate +
                ", deptCode='" + deptCode + '\'' +
                ", location='" + location + '\'' +
                ", riderNum=" + riderNum +
                ", status='" + status + '\'' +
                '}';
    }
}
