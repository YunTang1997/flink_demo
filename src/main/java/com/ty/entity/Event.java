package com.ty.entity;

import java.sql.Timestamp;
import java.util.Date;

/**
 * @author tangyun
 * @date 2022/4/4 5:27 下午
 */
public class Event {

    private String name;

    private String url;

    private Long timeStamp;

    public Event(){}

    public Event(String name, String url, Long timeStamp) {
        this.name = name;
        this.url = url;
        this.timeStamp = timeStamp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", timeStamp=" + new Timestamp(timeStamp) +
                '}';
    }
}
