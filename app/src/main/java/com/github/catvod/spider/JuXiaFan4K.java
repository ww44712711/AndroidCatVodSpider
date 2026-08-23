package com.github.catvod.spider;

import android.content.Context;

import com.github.catvod.bean.Class;
import com.github.catvod.bean.Filter;
import com.github.catvod.bean.Result;
import com.github.catvod.bean.Vod;
import com.github.catvod.crawler.Spider;
import com.github.catvod.net.OkHttp;
import com.github.catvod.utils.Json;
import com.github.catvod.utils.Util;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * 剧下饭4K影视
 *
 * 站点接口签名: AES-256-ECB / PKCS5Padding
 *   key      = kZ6fT8oF6oM8eX6lF7eH2rJ3pW7gW0kC
 *   明文     = 参数按 key 字典序升序拼接 k1=v1&k2=v2
 *   datasign = base64(AES_ECB_encrypt(PKCS5_pad(明文)))
 *
 * 接口一览:
 *   POST /api/v1/video/classifies     无 body    分类 + 筛选项
 *   POST /api/v1/video/recommended    无 body    首页推荐
 *   POST /api/v1/video/index          JSON       分类列表(免签)
 *   POST /api/v1/video/search         JSON       搜索(免签, pageSize 固定 10)
 *   POST /api/v1/video/videoDetails   form+签名   详情 + 播放源
 *   POST /api/v1/player/analysisUrl   form+签名   解析真实播放地址
 */
public class JuXiaFan4K extends Spider {

    private static final String AES_KEY = "kZ6fT8oF6oM8eX6lF7eH2rJ3pW7gW0kC";
    private static final String UA = "okhttp/5.3.2";

    private static final String[] HOSTS = {
            "http://manian.jugaoqing.com",
            "http://manian.juxiafan.com",
            "http://manian.juyongjiu.com",
            "http://manian.zhuiju666.com",
            "http://manian.jumianfei.com",
    };

    /** 详情页 vod_id 内拼接 name 的分隔符 */
    private static final String ID_SEP = "___";
    /** 播放地址内 sourceCode 与 playerCode 的分隔符 */
    private static final String PLAY_SEP = ":::";

    private static final Pattern VIDEO_EXT =
            Pattern.compile("\\.(m3u8|mp4|flv|mkv|ts)(\\?|$)", Pattern.CASE_INSENSITIVE);

    /** 搜索接口固定每页 10 条，服务端忽略 pageSize */
    private static final int SEARCH_LIMIT = 10;
    private static final int CATEGORY_LIMIT = 40;

    private String host = HOSTS[0];

    // ==================== 生命周期 ====================

    @Override
    public void init(Context context, String extend) throws Exception {
        super.init(context, extend);
        JsonObject ext = Json.safeObject(extend);
        if (ext.has("host") && !ext.get("host").isJsonNull()) {
            String h = ext.get("host").getAsString().trim();
            if (h.startsWith("http")) host = rstrip(h);
        }
        if (!alive(host)) {
            for (String h : HOSTS) {
                if (!h.equals(host) && alive(h)) {
                    host = h;
                    break;
                }
            }
        }
    }

    @Override
    public boolean isVideoFormat(String url) {
        return url != null && VIDEO_EXT.matcher(url).find();
    }

    @Override
    public boolean manualVideoCheck() {
        return false;
    }

    // ==================== 首页 ====================

    @Override
    public String homeContent(boolean filter) throws Exception {
        List<Class> classes = new ArrayList<>();
        LinkedHashMap<String, List<Filter>> filters = new LinkedHashMap<>();
        List<Vod> list = new ArrayList<>();

        JsonObject resp = postNoBody("/api/v1/video/classifies");
        if (!ok(resp)) return Result.string(classes, list);

        JsonArray cats = optArray(resp, "data");
        List<JsonObject> sorted = new ArrayList<>();
        for (JsonElement e : cats) if (e.isJsonObject()) sorted.add(e.getAsJsonObject());
        Collections.sort(sorted, new Comparator<JsonObject>() {
            @Override
            public int compare(JsonObject a, JsonObject b) {
                return Integer.compare(optInt(a, "sort", 99), optInt(b, "sort", 99));
            }
        });

        String firstTid = null;
        for (JsonObject cat : sorted) {
            String tid = optString(cat, "id");
            String name = optString(cat, "name");
            // 直播分类走的是另一套接口，这里不支持
            if (tid.isEmpty() || name.isEmpty() || name.contains("直播")) continue;
            classes.add(new Class(tid, name));
            if (firstTid == null) firstTid = tid;
            if (filter) {
                List<Filter> ft = buildFilter(optObject(cat, "extend"));
                if (!ft.isEmpty()) filters.put(tid, ft);
            }
        }

        if (firstTid != null) {
            JsonObject data = apiIndex(firstTid, 1, 30, null);
            if (data != null) {
                for (JsonElement e : optArray(data, "list")) {
                    if (e.isJsonObject()) list.add(toVod(e.getAsJsonObject()));
                }
            }
        }
        return Result.string(classes, list, filters);
    }

    @Override
    public String homeVideoContent() throws Exception {
        List<Vod> list = new ArrayList<>();
        JsonObject resp = postNoBody("/api/v1/video/recommended");
        if (!ok(resp)) return Result.string(list);
        for (JsonElement group : optArray(resp, "data")) {
            if (!group.isJsonObject()) continue;
            JsonArray videos = optArray(group.getAsJsonObject(), "videos");
            int n = 0;
            for (JsonElement v : videos) {
                if (n++ >= 10) break;
                if (v.isJsonObject()) list.add(toVod(v.getAsJsonObject()));
            }
        }
        return Result.string(list);
    }

    // ==================== 分类 ====================

    @Override
    public String categoryContent(String tid, String pg, boolean filter, HashMap<String, String> extend) throws Exception {
        int page = parseInt(pg, 1);
        List<Vod> list = new ArrayList<>();
        int total = 0, count = page, limit = CATEGORY_LIMIT;

        JsonObject data = apiIndex(tid, page, CATEGORY_LIMIT, extend);
        if (data != null) {
            for (JsonElement e : optArray(data, "list")) {
                if (e.isJsonObject()) list.add(toVod(e.getAsJsonObject()));
            }
            limit = optInt(data, "limit", CATEGORY_LIMIT);
            total = optInt(data, "totalCount", 0);
            count = optInt(data, "totalPage", page);
        }
        return Result.get().vod(list).page(page, count, limit, total).string();
    }

    // ==================== 详情 ====================

    @Override
    public String detailContent(List<String> ids) throws Exception {
        String raw = ids == null || ids.isEmpty() ? "" : ids.get(0);
        String[] split = splitId(raw);
        String vid = split[0], fallbackName = split[1];

        JsonObject detail = apiDetail(vid);
        if (detail == null) {
            Vod vod = new Vod();
            vod.setVodId(raw);
            vod.setVodName(fallbackName.isEmpty() ? vid : fallbackName);
            vod.setVodPlayFrom("提示");
            vod.setVodPlayUrl("详情获取失败$");
            return Result.string(vod);
        }

        List<String> playFrom = new ArrayList<>();
        List<String> playUrl = new ArrayList<>();

        for (JsonElement se : optArray(detail, "playerSource")) {
            if (!se.isJsonObject()) continue;
            JsonObject src = se.getAsJsonObject();
            if (optInt(src, "state", 0) != 1) continue;

            String srcCode = optString(src, "sourceCode");
            String srcName = optString(src, "sourceName");
            if (srcName.isEmpty()) srcName = srcCode.isEmpty() ? "未知" : srcCode;

            JsonArray eps = optArray(src, "episodes");
            if (eps.size() == 0) continue;
            boolean direct = optInt(src, "direct", 0) == 1;

            List<String> epList = new ArrayList<>();
            for (int i = 0; i < eps.size(); i++) {
                if (!eps.get(i).isJsonObject()) continue;
                JsonObject ep = eps.get(i).getAsJsonObject();
                String code = optString(ep, "playerCode");
                if (code.isEmpty()) continue;
                String epName = optString(ep, "episodeName");
                if (epName.isEmpty()) epName = String.valueOf(i + 1);
                // $ 与 # 是 vod_play_url 的分隔符，必须清掉
                epName = epName.replace("$", " ").replace("#", " ");
                if (direct && code.startsWith("http")) {
                    epList.add(epName + "$" + code);
                } else {
                    epList.add(epName + "$" + srcCode + PLAY_SEP + code);
                }
            }
            if (epList.isEmpty()) continue;
            playFrom.add(srcName.replace("$", " ").replace("#", " "));
            playUrl.add(join("#", epList));
        }

        if (playFrom.isEmpty()) {
            playFrom.add("提示");
            playUrl.add("暂无可用源$");
        }

        String year = optString(detail, "year");
        if (year.isEmpty()) year = optString(detail, "pubDate");

        Vod vod = new Vod();
        vod.setVodId(raw);
        vod.setVodName(pick(optString(detail, "name"), fallbackName));
        vod.setVodPic(optString(detail, "videoPic"));
        vod.setVodYear(year);
        vod.setVodArea(optString(detail, "area"));
        vod.setTypeName(optString(detail, "classify"));
        vod.setVodActor(optString(detail, "actor"));
        vod.setVodDirector(optString(detail, "director"));
        vod.setVodContent(optString(detail, "content"));
        vod.setVodRemarks(buildRemarks(detail));
        vod.setVodPlayFrom(join("$$$", playFrom));
        vod.setVodPlayUrl(join("$$$", playUrl));
        return Result.string(vod);
    }

    // ==================== 搜索 ====================

    @Override
    public String searchContent(String key, boolean quick) throws Exception {
        return searchContent(key, quick, "1");
    }

    @Override
    public String searchContent(String key, boolean quick, String pg) throws Exception {
        int page = parseInt(pg, 1);
        List<Vod> list = new ArrayList<>();
        JsonObject payload = new JsonObject();
        payload.addProperty("keyword", key);
        payload.addProperty("pageNum", page);
        payload.addProperty("pageSize", SEARCH_LIMIT);

        JsonObject resp = postJson("/api/v1/video/search", payload.toString());
        int count = page, total = 0, limit = SEARCH_LIMIT;
        if (ok(resp)) {
            JsonObject data = optObject(resp, "data");
            for (JsonElement e : optArray(data, "list")) {
                if (e.isJsonObject()) list.add(toVod(e.getAsJsonObject()));
            }
            limit = optInt(data, "limit", SEARCH_LIMIT);
            total = optInt(data, "totalCount", 0);
            count = optInt(data, "totalPage", page);
        }
        return Result.get().vod(list).page(page, count, limit, total).string();
    }

    // ==================== 播放 ====================

    @Override
    public String playerContent(String flag, String id, List<String> vipFlags) throws Exception {
        Map<String, String> header = new HashMap<>();
        header.put("User-Agent", Util.CHROME);

        String value = id == null ? "" : id;

        if (value.contains(PLAY_SEP)) {
            int at = value.indexOf(PLAY_SEP);
            String srcCode = value.substring(0, at);
            String playerCode = value.substring(at + PLAY_SEP.length());

            // 已经是直链，不必再解析
            if (playerCode.startsWith("http") && isVideoFormat(playerCode)) {
                return Result.get().url(playerCode).header(header).parse(0).string();
            }
            String real = apiAnalysis(playerCode, srcCode);
            if (!real.isEmpty() && real.startsWith("http")) {
                // analysisUrl 返回的一定是可直接播放的媒体地址（实测 mp4 / m3u8 裸流），
                // 但不少地址不带 .mp4/.m3u8 后缀（如对象存储签名 URL、m3u8 转发接口），
                // 靠后缀判断会误判成网页而走嗅探。这里统一 parse=0 直接交播放器。
                return Result.get().url(real).header(header).parse(0).string();
            }
            // 解析失败，是网页地址就交给播放器嗅探
            if (playerCode.startsWith("http")) {
                return Result.get().url(playerCode).header(header).parse(1).string();
            }
            return Result.error("解析失败");
        }

        if (value.startsWith("http")) {
            return Result.get().url(value).header(header).parse(isVideoFormat(value) ? 0 : 1).string();
        }
        return Result.error("无效播放地址");
    }

    // ==================== 接口封装 ====================

    private JsonObject apiIndex(String tid, int page, int size, Map<String, String> extend) {
        JsonObject payload = new JsonObject();
        payload.addProperty("pageNum", page);
        payload.addProperty("pageSize", size);
        payload.addProperty("typeId", parseInt(tid, 0));
        if (extend != null) {
            // 服务端只认 area / classify / year 三个筛选键
            putIfPresent(payload, extend, "area", "area");
            putIfPresent(payload, extend, "classify", "classify");
            putIfPresent(payload, extend, "year", "year");
        }
        JsonObject resp = postJson("/api/v1/video/index", payload.toString());
        return ok(resp) ? optObject(resp, "data") : null;
    }

    private JsonObject apiDetail(String vid) {
        Map<String, String> params = new TreeMap<>();
        params.put("id", vid);
        params.put("timestamp", timestamp());
        String sign = sign(params);
        if (sign.isEmpty()) return null;
        Map<String, String> form = new HashMap<>(params);
        form.put("datasign", sign);
        JsonObject resp = postForm("/api/v1/video/videoDetails", form);
        return ok(resp) ? optObject(resp, "data") : null;
    }

    private String apiAnalysis(String code, String srcCode) {
        Map<String, String> params = new TreeMap<>();
        params.put("code", code);
        params.put("from", srcCode);
        params.put("timestamp", timestamp());
        String sign = sign(params);
        if (sign.isEmpty()) return "";
        Map<String, String> form = new HashMap<>(params);
        form.put("datasign", sign);
        JsonObject resp = postForm("/api/v1/player/analysisUrl", form);
        if (!ok(resp)) return "";
        JsonElement data = resp.get("data");
        if (data == null || data.isJsonNull()) return "";
        if (data.isJsonPrimitive()) return data.getAsString();
        if (data.isJsonObject()) return optString(data.getAsJsonObject(), "url");
        return "";
    }

    // ==================== 签名 ====================

    /** 参数按 key 升序拼 k=v&k=v -> AES-256-ECB/PKCS5 -> base64 */
    private String sign(Map<String, String> params) {
        try {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : new TreeMap<>(params).entrySet()) {
                if (sb.length() > 0) sb.append('&');
                sb.append(entry.getKey()).append('=').append(entry.getValue());
            }
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(AES_KEY.getBytes("UTF-8"), "AES"));
            byte[] out = cipher.doFinal(sb.toString().getBytes("UTF-8"));
            return base64(out);
        } catch (Exception e) {
            return "";
        }
    }

    private static String base64(byte[] data) {
        return android.util.Base64.encodeToString(data, android.util.Base64.NO_WRAP);
    }

    private static String timestamp() {
        return String.valueOf(System.currentTimeMillis() / 1000);
    }

    // ==================== 网络 ====================

    private boolean alive(String h) {
        try {
            String body = OkHttp.post(h + "/api/v1/video/classifies", "{}", jsonHeader()).getBody();
            return body.contains("\"code\":200");
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 无 body 的接口。
     * 注意: 必须发 application/json，不能用 form-urlencoded。
     * 服务端把 form-urlencoded 视为「签名请求」，未带 datasign 会直接返回 "Signature failed"。
     */
    private JsonObject postNoBody(String path) {
        return Json.safeObject(OkHttp.post(host + path, "{}", jsonHeader()).getBody());
    }

    private JsonObject postJson(String path, String json) {
        return Json.safeObject(OkHttp.post(host + path, json, jsonHeader()).getBody());
    }

    /** 带签名的接口走 form-urlencoded（OkHttp 传 Map 即 FormBody） */
    private JsonObject postForm(String path, Map<String, String> form) {
        Map<String, String> header = new HashMap<>();
        header.put("User-Agent", UA);
        return Json.safeObject(OkHttp.post(host + path, form, header).getBody());
    }

    private Map<String, String> jsonHeader() {
        Map<String, String> header = new HashMap<>();
        header.put("User-Agent", UA);
        header.put("Content-Type", "application/json; charset=UTF-8");
        return header;
    }

    // ==================== 工具 ====================

    private List<Filter> buildFilter(JsonObject ext) {
        List<Filter> filters = new ArrayList<>();
        if (ext == null) return filters;
        String[][] defs = {
                {"class", "classify", "类型", "30"},
                {"area", "area", "地区", "30"},
                {"year", "year", "年份", "16"},
        };
        for (String[] def : defs) {
            String raw = optString(ext, def[0]);
            if (raw.isEmpty()) continue;
            int cap = parseInt(def[3], 20);
            List<Filter.Value> values = new ArrayList<>();
            values.add(new Filter.Value("全部", ""));
            for (String item : raw.split(",")) {
                String v = item.trim();
                if (v.isEmpty()) continue;
                values.add(new Filter.Value(v, v));
                if (values.size() > cap) break;
            }
            if (values.size() > 1) filters.add(new Filter(def[1], def[2], values));
        }
        return filters;
    }

    private Vod toVod(JsonObject v) {
        String vid = optString(v, "id");
        String name = optString(v, "name");
        String remarks = optString(v, "remarks");
        if (remarks.isEmpty()) {
            String score = optString(v, "score");
            if (!score.isEmpty() && !"0".equals(score) && !"0.0".equals(score)) remarks = "评分 " + score;
        }
        Vod vod = new Vod();
        // 详情接口只回 id，把 name 一起带上做兜底展示
        vod.setVodId(name.isEmpty() ? vid : vid + ID_SEP + name);
        vod.setVodName(name);
        vod.setVodPic(optString(v, "videoPic"));
        vod.setVodYear(optString(v, "year"));
        vod.setVodRemarks(remarks);
        return vod;
    }

    private String buildRemarks(JsonObject detail) {
        String remarks = optString(detail, "remarks");
        if (!remarks.isEmpty()) return remarks;
        List<String> parts = new ArrayList<>();
        String score = optString(detail, "score");
        if (!score.isEmpty() && !"0".equals(score) && !"0.0".equals(score)) parts.add("评分 " + score);
        String duration = optString(detail, "duration");
        if (!duration.isEmpty()) parts.add(duration);
        return join(" ", parts);
    }

    private String[] splitId(String id) {
        String s = id == null ? "" : id;
        int at = s.indexOf(ID_SEP);
        if (at < 0) return new String[]{s, ""};
        return new String[]{s.substring(0, at), s.substring(at + ID_SEP.length())};
    }

    private static void putIfPresent(JsonObject payload, Map<String, String> extend, String key, String out) {
        String v = extend.get(key);
        if (v != null && !v.trim().isEmpty()) payload.addProperty(out, v.trim());
    }

    private static boolean ok(JsonObject obj) {
        return obj != null && optInt(obj, "code", 0) == 200;
    }

    private static JsonArray optArray(JsonObject obj, String key) {
        if (obj == null) return new JsonArray();
        JsonElement e = obj.get(key);
        return e != null && e.isJsonArray() ? e.getAsJsonArray() : new JsonArray();
    }

    private static JsonObject optObject(JsonObject obj, String key) {
        if (obj == null) return new JsonObject();
        JsonElement e = obj.get(key);
        return e != null && e.isJsonObject() ? e.getAsJsonObject() : new JsonObject();
    }

    private static String optString(JsonObject obj, String key) {
        if (obj == null) return "";
        JsonElement e = obj.get(key);
        if (e == null || e.isJsonNull() || !e.isJsonPrimitive()) return "";
        String s = e.getAsString();
        return s == null ? "" : s.trim();
    }

    private static int optInt(JsonObject obj, String key, int def) {
        if (obj == null) return def;
        JsonElement e = obj.get(key);
        if (e == null || e.isJsonNull() || !e.isJsonPrimitive()) return def;
        try {
            return e.getAsInt();
        } catch (Exception ignored) {
            return def;
        }
    }

    private static int parseInt(String s, int def) {
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception e) {
            return def;
        }
    }

    private static String join(String sep, List<String> items) {
        StringBuilder sb = new StringBuilder();
        for (String item : items) {
            if (sb.length() > 0) sb.append(sep);
            sb.append(item);
        }
        return sb.toString();
    }

    private static String pick(String a, String b) {
        return a != null && !a.isEmpty() ? a : (b == null ? "" : b);
    }

    private static String rstrip(String s) {
        String out = s;
        while (out.endsWith("/")) out = out.substring(0, out.length() - 1);
        return out;
    }
}
