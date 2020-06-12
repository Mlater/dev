package com.later.utils;

/**
 * @description:
 * @author: Liu Jun Jun
 * @create: 2020-06-10 15:21
 **/
import com.alibaba.fastjson.JSONObject;


public class ParseJsonData {

    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            return null;
        }
    }
}
