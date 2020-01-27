WITH new_subs AS (
    SELECT DISTINCT clientuserid
    , createddate
    FROM svot.MPP_SUBSCRIPTION
    WHERE serviceid IN (select service_id from
            dev_edw.SVOT.SUBSCRIPTION_SERVICE_LOOKUP where paid_unpaid = 'Paid' and source_file_name = 'thestar')
    AND CREATEDDATE >= '2019-05-20'
    )


, cookies_for_janrain AS (
    SELECT visitor_site_id, janrain_uuid
    FROM svot.PAGEVIEW_P
    WHERE ts_action >= '2019-04-20'
    AND apikey = 'thestar.com'
    )

, full_ids AS (
    SELECT DISTINCT visitor_site_id, ns.clientuserid as janrain_uuid, createddate
    FROM new_subs ns
    LEFT JOIN cookies_for_janrain cj
        ON cj.janrain_uuid = ns.clientuserid
    )

, possible_pvs AS (
    SELECT DISTINCT visitor_site_id, janrain_uuid, metadata_canonical_url, ts_action, sref_category, campaign_id, utm_source, visitor_ip
    FROM svot.PAGEVIEW_P
    WHERE ts_action >= '2019-04-20'
    AND apikey = 'thestar.com'
    AND metadata_page_type = 'post'
    AND (regexp_like(split_part(metadata_canonical_url,'/',7),'[0-9][0-9].*')
    OR metadata_canonical_url LIKE '%projects.thestar%')
    )

, ds AS (
    SELECT ids.janrain_uuid, metadata_canonical_url, sref_category, campaign_id, utm_source, ts_action, createddate, visitor_ip
    FROM possible_pvs ap
    INNER JOIN full_ids ids
        ON ap.janrain_uuid = ids.janrain_uuid
        AND createddate > ap.ts_action
        AND dateadd('day',-30,createddate) <= ts_action
    UNION ALL
    SELECT ids.janrain_uuid, metadata_canonical_url, sref_category, campaign_id, utm_source, ts_action, createddate, visitor_ip
    FROM possible_pvs ap
    INNER JOIN full_ids ids
        ON ap.visitor_site_id = ids.visitor_site_id
        AND createddate > ap.ts_action
        AND dateadd('day',-30,createddate) <= ts_action
    )

, fs AS (
    SELECT janrain_uuid, metadata_canonical_url, sref_category, campaign_id, utm_source, ts_action, createddate, visitor_ip
    , rank() OVER (PARTITION BY janrain_uuid ORDER BY ts_action desc) as click_rank
    FROM ds
    GROUP BY 1,2,3,4,5,6,7,8
    )

,  top_5 AS (
    SELECT *
    FROM fs
    WHERE click_rank <= 5
    )

, par_arts AS (
    SELECT metadata_canonical_url, METADATA_TITLE, dateadd('ms',metadata_pub_date_tmsp,0::timestamp) as pub_time
    , max(ts_action) as max_ts_action
    FROM svot.PAGEVIEW_P
    WHERE apikey = 'thestar.com'
    AND ts_action >= '2019-04-20'
    AND metadata_pub_date_tmsp IS NOT NULL
    AND metadata_page_type = 'post'
    GROUP BY 1,2,3
    )

,title AS (
  SELECT *
  FROM (SELECT metadata_canonical_url, metadata_title, pub_time
  , rank() OVER (PARTITION BY metadata_canonical_url ORDER BY max_ts_action desc) as recent
  FROM par_arts
  ) p
  WHERE recent = 1
  )

, authors AS (
    SELECT DISTINCT lower(REGEXP_REPLACE (SECTION_AUTHOR,'’','\'')) as SECTION_AUTHOR, POST_PAGE_URL
    FROM svot.THESTARBROWSER_HIT_DATA_P
    WHERE SECTION_AUTHOR IS NOT NULL
    AND data_date  >= '2019-04-20'
    )

, article_authors AS (
    SELECT DISTINCT METADATA_CANONICAL_URL, METADATA_TITLE, pub_time, author, team
    FROM authors
    INNER JOIN title
       ON metadata_canonical_url = POST_PAGE_URL
    LEFT JOIN DEV_EDW.SVOT.AUTHOR_TEAM_BY_BRAND t
      ON POSITION(lower(t.author),lower(SECTION_AUTHOR)) > 0
      AND pub_time BETWEEN t.eff_ts AND t.exp_ts
    )


, clicks AS (
    SELECT t.*, a.author, team, dateadd('sec',5, ts_action) as plus_5, dateadd('sec',-5, ts_action) as minus_5
    FROM top_5 t
    INNER JOIN article_authors a
        ON t.METADATA_CANONICAL_URL = a.METADATA_CANONICAL_URL
    )

, wall_events AS (
    SELECT split_part(PAGE_URL,'.html',1)||'.html' as url
    , COOKIE, IP, DATE_TIME
    , CASE WHEN page_event_var2 LIKE '%lock)' THEN 'locked_content'
      ELSE 'wall_reached'
      END AS wall_type
    FROM svot.THESTARBROWSER_HIT_DATA_P
    WHERE DATE_TIME >= '2019-04-20'
    AND PAGE_EVENT_VAR2 LIKE 'paywall: wall%'
    )

SELECT wall_type, COUNT(DISTINCT janrain_uuid)
FROM (SELECT DISTINCT c.*, wall_type
FROM clicks c
LEFT JOIN wall_events w
    ON c.METADATA_CANONICAL_URL = w.url
    AND ip = visitor_ip
    AND date_time BETWEEN minus_5 AND plus_5
    )x
    GROUP BY 1

;
