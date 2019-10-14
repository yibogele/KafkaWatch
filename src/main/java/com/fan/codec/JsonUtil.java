package com.fan.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.commons.lang.StringUtils;
//import javax.servlet.http.HttpServletRequest;

/**
 * Json帮助类
 * 主要处理json与对象转换
 * <p>
 * Author hanpengcheng
 */
public class JsonUtil {

    private final static ObjectMapper objectMapper = new ObjectMapper();

    private JsonUtil() {
    }

    public static ObjectMapper getInstance() {
        return objectMapper;
    }

    /**
     * javaBean,list,array convert to json string
     */
    public static String obj2json(Object obj) {
        try {
            objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * json string convert to javaBean
     */
    public static <T> T json2pojo(String jsonStr, Class<T> clazz)
            throws Exception {
        return objectMapper.readValue(jsonStr, clazz);
    }

    /**
     * json string convert to map
     */
    public static <T> Map<String, Object> json2map(String jsonStr)
            throws Exception {
        return StringUtils.isNotBlank(jsonStr) ? objectMapper.readValue(jsonStr, Map.class) : new HashMap<String, Object>();
    }

    /**
     * json string convert to map with javaBean
     */
    static <T> Map<String, T> json2map(String jsonStr, Class<T> clazz)
            throws Exception {
        Map<String, Map<String, Object>> map = objectMapper.readValue(jsonStr,
                new TypeReference<Map<String, T>>() {
                });
        Map<String, T> result = new HashMap<String, T>();
        for (Map.Entry<String, Map<String, Object>> entry : map.entrySet()) {
            result.put(entry.getKey(), map2pojo(entry.getValue(), clazz));
        }
        return result;
    }

    /**
     * json array string convert to list with javaBean
     */
    public static <T> List<T> json2list(String jsonArrayStr, Class<T> clazz)
            throws Exception {
        List<Map<String, Object>> list = objectMapper.readValue(jsonArrayStr,
                new TypeReference<List<T>>() {
                });
        List<T> result = new ArrayList<T>();
        for (Map<String, Object> map : list) {
            result.add(map2pojo(map, clazz));
        }
        return result;
    }

    /**
     * json array string convert to list with javaBean
     */
    public static <T> List<T> mapList2ObjList(List list, Class<T> clazz) {
        List<T> ObjectList = new ArrayList<T>(list.size());
        T t;
        for (Object obj : list) {
            t = map2pojo((Map) obj, clazz);
            ObjectList.add(t);
        }
        return ObjectList;
    }

    /**
     * map convert to javaBean
     */
    private static <T> T map2pojo(Map map, Class<T> clazz) {
        return objectMapper.convertValue(map, clazz);
    }

    /*
     * 根据request获取json字符串
     *
     * @param request
     * @return
     * @throws IOException
     */
//    public static String getJsonByRequest(HttpServletRequest request) throws IOException {
//        String str, wholeStr = "";
//        try {
//            BufferedReader br = request.getReader();
//            while ((str = br.readLine()) != null) {
//                wholeStr += str.trim();
//            }
//            br.close();
//
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return String.valueOf(wholeStr);
//    }

    /*
     * 根据request和对象类型返回对象实体
     *
     * @param request
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
//    public static <T> T getObjectByRequest(HttpServletRequest request, Class<T> clazz) throws Exception {
//        String jsonStr = getJsonByRequest(request);
//        return json2pojo(jsonStr, clazz);
//    }
}
