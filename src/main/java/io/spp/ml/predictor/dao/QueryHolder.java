package io.spp.ml.predictor.dao;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class QueryHolder
{
    private static final Map<String, String> queryMap = new ConcurrentHashMap<>();

    public static void readQueries(String queryFilePath, String queryIdPrefix, String queryIdPostfix)
    {
        readQueries(queryFilePath, queryIdPrefix, queryIdPostfix, null);       
    }
    
    public static void readQueries(String queryFilePath, String queryIdPrefix, String queryIdPostfix, String queryIdSuffixForUnformatted)
    {

        log.debug("Start loading queries from {}", queryFilePath);
        
        try (
                InputStream is = QueryHolder.class.getClassLoader().getResourceAsStream(queryFilePath); 
                BufferedReader br = new BufferedReader(new InputStreamReader(is))
                        )
        {

            String queryId = null;
            String query = null;
            String line = null;
            boolean isFormatted = true;

            while ((line = br.readLine()) != null)
            {
                if (line.trim().startsWith(queryIdPrefix))
                {
                    // put previous query in map
                    if (queryId != null)
                    {
                        putQuery(queryFilePath, queryId, query);
                    }

                    // prepare next query
                    queryId = line.trim().replace(queryIdPostfix, "").replace(queryIdPrefix, "");
                    isFormatted = Objects.nonNull(queryIdSuffixForUnformatted) && !queryId.endsWith(queryIdSuffixForUnformatted);
                    query = "";
                }
                else if (!"".equals(line))
                {
                    query += line + (isFormatted ? "\n" : "");
                }
            }

            // put last query into the map
            if (queryId != null)
            {
                putQuery(queryFilePath, queryId, query);
            }
            
            log.info("Finished loading queries from {}", queryFilePath);
        }
        catch (IOException io)
        {
            log.error("IOException, details: " + io.getMessage());
        }
    }

    private static void putQuery(String queryFilePath, String tag, String query)
    {
        String queryTag = getQueryTag(queryFilePath, tag);

        if (queryMap.containsKey(queryTag))
            throw new IllegalArgumentException(String.format("Query with filepath %s and tag %s mentioned twice. Please check either file or loading mechanism", queryFilePath, tag));

        queryMap.put(queryTag, query);
    }

    public static String getQuery(String queryFilePath, String tag)
    {
        String queryTag = getQueryTag(queryFilePath, tag);
        if (!queryMap.containsKey(queryTag))
            throw new IllegalArgumentException(String.format("Query with filepath %s and tag %s not found. Please check either file or loading mechanism", queryFilePath, tag));

        return queryMap.get(queryTag);
    }

    private static String getQueryTag(String queryFilePath, String tag)
    {
        return (queryFilePath + "[" + tag + "]").intern();
    }

}
