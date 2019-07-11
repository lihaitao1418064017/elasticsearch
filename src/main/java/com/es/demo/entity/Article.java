package com.es.demo.entity;

import io.searchbox.annotations.JestId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author LiHaitao
 * @description Article:
 * @date 2019/7/11 14:57
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Article {


    @JestId
    private int id;
    private String title;
    private String content;
    private String url;
    private Date pubdate;
    private String source;
    private String author;

}
