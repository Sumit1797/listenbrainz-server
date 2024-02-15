from datetime import datetime
from typing import List, Iterable

import sqlalchemy

SIMILARITY_ALGORITHM = "session_based_days_7500_session_300_contribution_3_threshold_15_limit_50_filter_True_skip_30"

# TODO: Choose similar artists according to mode


def lb_radio_artist(db_conn, seed_artist: str, max_similar_artists: int, num_recordings_per_artist: int, begin_percentage: float,
                    end_percentage: float) -> List[dict]:

    # The query requires a count, which is safe to leave 0
    seed_artist = (seed_artist, 0)

    result = db_conn.execute(
        sqlalchemy.text("""
         WITH mbids(mbid, score) AS (
                     VALUES :seed_artist
                 ), similar_artists AS (
                     SELECT CASE WHEN mbid0 = mbid::UUID THEN mbid1::TEXT ELSE mbid0::TEXT END AS similar_artist_mbid
                          , jsonb_object_field(metadata, :algorithm)::integer AS score
                       FROM similarity.artist
                       JOIN mbids
                         ON TRUE
                      WHERE (mbid0 = mbid::UUID OR mbid1 = mbid::UUID)
                        AND metadata ? :algorithm
                   ORDER BY score DESC
                      LIMIT :max_similar_artists
                 ), similar_artists_and_orig_artist AS (
                     SELECT *
                       FROM similar_artists
                      UNION
                     SELECT *
                       FROM mbids
                 ), top_recordings AS (
                     SELECT sa.similar_artist_mbid
                          , pr.recording_mbid
                          , total_listen_count
                          , PERCENT_RANK() OVER (PARTITION BY similar_artist_mbid ORDER BY sa.similar_artist_mbid, total_listen_count ) AS rank
                       FROM popularity.top_recording pr
                       JOIN similar_artists_and_orig_artist sa
                         ON sa.similar_artist_mbid::UUID = pr.artist_mbid
                   GROUP BY sa.similar_artist_mbid, pr.total_listen_count, pr.recording_mbid
                 ), randomize AS (
                     SELECT similar_artist_mbid
                          , recording_mbid    
                          , total_listen_count
                          , rank                                                                                              
                          , ROW_NUMBER() OVER (PARTITION BY similar_artist_mbid ORDER BY RANDOM()) AS rownum
                       FROM top_recordings
                      WHERE rank >= :begin_percentage and rank < :end_percentage   -- select the range of results here
                 )
                     SELECT similar_artist_mbid
                          , recording_mbid
                          , total_listen_count
                          , rank
                          , rownum
                       FROM randomize
                      WHERE rownum < :num_recordings_per_artist"""), {
            "seed_artist": seed_artist,
            "algorithm": SIMILARITY_ALGORITHM,
            "max_similar_artists": max_similar_artists,
            "begin_percentage": begin_percentage,
            "end_percentage": end_percentage,
            "num_recordings_per_artist": num_recordings_per_artist
        })

    return result.mappings().all()
