package com;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TestParseUrlQueryString {

/**
* Parses an URL query string and returns a map with the parameter values.
* The URL query string is the part in the URL after the first '?' character up
* to an optional '#' character. It has the format "name=value&name=value&...".
* The map has the same structure as the one returned by
* javax.servlet.ServletRequest.getParameterMap().
* A parameter name may occur multiple times within the query string.
* For each parameter name, the map contains a string array with the parameter values.
* @param  s  an URL query string.
* @return    a map containing parameter names as keys and parameter values as map values.
* @author    Christian d'Heureuse, Inventec Informatik AG, Switzerland, www.source-code.biz.
*/
public static Map<String, String[]> parseUrlQueryString (String s) {
   if (s == null) return new HashMap<String, String[]>(0);
   // In map1 we use strings and ArrayLists to collect the parameter values.
   HashMap<String, Object> map1 = new HashMap<String, Object>();
   int p = 0;
   while (p < s.length()) {
      int p0 = p;
      while (p < s.length() && s.charAt(p) != '=' && s.charAt(p) != '&') p++;
        String name = urlDecode(s.substring(p0, p));
        if (p < s.length() && s.charAt(p) == '=') p++;
        p0 = p;
        while (p < s.length() && s.charAt(p) != '&') p++;
        String value = urlDecode(s.substring(p0, p));
        if (p < s.length() && s.charAt(p) == '&') p++;
        Object x = map1.get(name);
      if (x == null) {
         // The first value of each name is added directly as a string to the map.
         map1.put (name, value); }
       else if (x instanceof String) {
         // For multiple values, we use an ArrayList.
         ArrayList<String> a = new ArrayList<String>();
         a.add ((String)x);
         a.add (value);
         map1.put (name, a); }
       else {
         @SuppressWarnings("unchecked")
            ArrayList<String> a = (ArrayList<String>)x;
            a.add (value); }}
   // Copy map1 to map2. Map2 uses string arrays to store the parameter values.
   HashMap<String, String[]> map2 = new HashMap<String, String[]>(map1.size());
   for (Map.Entry<String, Object> e : map1.entrySet()) {
      String name = e.getKey();
      Object x = e.getValue();
      String[] v;
      if (x instanceof String) {
         v = new String[]{(String)x}; }
       else {
         @SuppressWarnings("unchecked")
            ArrayList<String> a = (ArrayList<String>)x;
         v = new String[a.size()];
         v = a.toArray(v); }
      map2.put (name, v); }
   return map2; }

private static String urlDecode (String s) {
   try {
      return URLDecoder.decode(s, "UTF-8"); }
    catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Error in urlDecode.", e); }}

private static void test1() throws Exception {
   Map<String, String[]> map = parseUrlQueryString("parm2=abc&parm1=123&parm2=xyz&parm3=1&parm3=2&parm3=3");
   if (map.size() != 3) throw new Exception();
   String[] v;
   v = map.get("parm1");
   if (v == null || v.length != 1 || !"123".equals(v[0])) throw new Exception();
   v = map.get("parm2");
   if (v == null || v.length != 2 || !"abc".equals(v[0]) || !"xyz".equals(v[1])) throw new Exception();
   v = map.get("parm3");
   if (v == null || v.length != 3 || !"1".equals(v[0]) || !"2".equals(v[1]) || !"3".equals(v[2])) throw new Exception(); }

private static void test2() throws Exception {
   URI uri = new URI("http://www.source-code.biz/test?p%3D1=hello%26gr%C3%BCezi");
   String s = uri.getRawQuery();
   Map<String, String[]> map = parseUrlQueryString(s);
   String[] v = map.get("p=1");
   if (v == null || v.length != 1 || !"hello&grüezi".equals(v[0])) throw new Exception(); }

public static void main (String[] args) throws Exception {
   test1();
   test2();
   System.out.println ("ok"); }

}