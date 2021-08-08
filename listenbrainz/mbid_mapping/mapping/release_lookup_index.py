from icecream import ic

import sys
import re
import time
import datetime
import json

from unidecode import unidecode
import psycopg2
import pysolr

import config
from mapping.utils import log


BATCH_SIZE = 100000 
SOLR_HOST = "listenbrainz-solr"
SOLR_PORT = 8983
SOLR_CORE = "release-index"


def build_release_lookup_index():

    def set_recording_data(data, row):
        data["recording_names"].append(("%d " % rec_index) + row["recording_name"])
        data["release_ac_names"].append(("%d " % rec_index) + " ".join(row["artist_credit_names"]))

    def append_to_docs(data):
        data["count"] = len(data["recording_names"])
        data["recording_names"] = " ".join(data["recording_names"])
        data["release_ac_names"] = " ".join(data["release_ac_names"])

        if data['ac_id'] != 1:
            del data["release_ac_names"]

        docs.append(data)

    solr = pysolr.Solr('http://%s:%d/solr/%s' % (SOLR_HOST, SOLR_PORT, SOLR_CORE), always_commit=True)

    with psycopg2.connect(config.MBID_MAPPING_DATABASE_URI) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

            query = ("""SELECT ac.id AS artist_credit_id,
                               ac.name AS artist_credit_name,
                               rel.gid AS release_mbid,
                               rel.name AS release_name,
                               m.position,
                               rec.name AS recording_name,
                               array_agg(acn2.name) AS artist_credit_names
                          FROM artist_credit ac
                          JOIN release rel
                            ON rel.artist_credit = ac.id
                          JOIN medium m
                            ON m.release = rel.id
                          JOIN track t
                            ON t.medium = m.id
                          JOIN recording rec
                            ON t.recording = rec.id
                          JOIN artist_credit_name acn2
                            ON rec.artist_credit = acn2.artist_credit
                      GROUP BY ac.id, ac.name, rel.gid, rel.name, m.position, t.position, rec.name""")

                        # VA example with compound AC
                        #WHERE rel.gid = 'ef35af78-a062-46df-aa3f-4fe440a0b806'

                        # Large VA and simple album
                        #WHERE rel.gid IN ('5fc28f73-4ccf-4b38-b96e-a8e706f388e5', 'f88d6d9b-9664-4c54-887c-2bd83248bc2c')

            log("Run query")
            curs.execute(query)

            docs = []
            batch_count = 0

            last_id = ""
            data = {}
            rec_index = 1
            log("Build index")
            for row in curs:
                if curs.rowcount == curs.rownumber:
                    if data:
                        set_recording_data(data, row)
                        append_to_docs(data)
                    break

                id = "%s-%d" % (str(row["release_mbid"]), row["position"])
                if last_id != id:
                    if data:
                        append_to_docs(data)

                    data = {
                        "id": id,
                        "title": row["release_name"],
                        "ac_id": row["artist_credit_id"],
                        "ac_name": row["artist_credit_name"],
                        "pos": row["position"],
                        "recording_names": [],
                        "release_ac_names": []
                    }
                    rec_index = 1

                    if len(docs) == BATCH_SIZE:
                        solr.add(docs)
                        docs = []
                        batch_count += 1

                        if batch_count % 10 == 0:
                            log("Added %d rows" % (BATCH_SIZE * batch_count))

                set_recording_data(data, row)
                last_id = id
                rec_index += 1

            if len(docs):
                solr.add(docs)

            log("Done!")
