from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 insert_query=None,
                 redshift_conn_id=None,
                 table_name=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.insert_query=insert_query
        self.table_name=table_name
        self.redshift_conn_id=redshift_conn_id

        
    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.insert_query is None:
            self.insert_query = ("""
                SELECT
                    md5(events.sessionid || events.start_time) songplay_id,
                    events.start_time, 
                    events.userid, 
                    events.level, 
                    songs.song_id, 
                    songs.artist_id, 
                    events.sessionid, 
                    events.location, 
                    events.useragent
                    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                FROM staging_events
                WHERE page='NextSong') events
                LEFT JOIN staging_songs songs
                ON events.song = songs.title
                    AND events.artist = songs.artist_name
                    AND events.length = songs.duration
                    """)

        redshift.run("""
                    INSERT INTO {}
                    {};""".format(self.table_name, self.insert_query))

