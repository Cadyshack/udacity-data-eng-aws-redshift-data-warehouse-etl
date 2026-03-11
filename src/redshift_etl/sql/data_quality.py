
# DATA QUALITY CHECKS
data_quality_checks = [
    # A. Row count checks — every table should have rows
    {
        "check_sql": "SELECT COUNT(*) FROM songplays;",
        "expected": lambda result: result[0] > 0,
        "description": "songplays table has rows"
    },
    {
        "check_sql": "SELECT COUNT(*) FROM users;",
        "expected": lambda result: result[0] > 0,
        "description": "users table has rows"
    },
    {
        "check_sql": "SELECT COUNT(*) FROM songs;",
        "expected": lambda result: result[0] > 0,
        "description": "songs table has rows"
    },
    {
        "check_sql": "SELECT COUNT(*) FROM artists;",
        "expected": lambda result: result[0] > 0,
        "description": "artists table has rows"
    },
    {
        "check_sql": "SELECT COUNT(*) FROM time;",
        "expected": lambda result: result[0] > 0,
        "description": "time table has rows"
    },

    # B. Referential integrity — users in songplays exist in users table
    {
        "check_sql": """
            SELECT COUNT(*)
            FROM songplays sp
            LEFT JOIN users u ON sp.user_id = u.user_id
            WHERE u.user_id IS NULL;
        """,
        "expected": lambda result: result[0] == 0,
        "description": "All songplays.user_id exist in users.user_id table"
    },
    {
        "check_sql": """
            SELECT COUNT(*)
            FROM songplays sp
            LEFT JOIN songs s ON sp.song_id = s.song_id
            WHERE sp.song_id IS NOT NULL AND s.song_id IS NULL;
        """,
        "expected": lambda result: result[0] == 0,
        "description": "All songplays.song_id exist in songs.song_id table"
    },
    {
        "check_sql": """
            SELECT COUNT(*)
            FROM songplays sp
            LEFT JOIN artists a ON sp.artist_id = a.artist_id
            WHERE sp.artist_id IS NOT NULL AND a.artist_id IS NULL;
        """,
        "expected": lambda result: result[0] == 0,
        "description": "All songplays.artist_id exist in artists.artist_id table"
    },
    {
        "check_sql": """
            SELECT COUNT(*)
            FROM songplays sp
            LEFT JOIN time t ON sp.start_time = t.start_time
            WHERE t.start_time IS NULL;
        """,
        "expected": lambda result: result[0] == 0,
        "description": "All songplays.start_time exist in time.start_time table"
    },
    {
        "check_sql": """
            SELECT COUNT(*)
            FROM songs s
            LEFT JOIN artists a ON s.artist_id = a.artist_id
            WHERE a.artist_id IS NULL;
        """,
        "expected": lambda result: result[0] == 0,
        "description": "All songs.artist_id exist in artists.artist_id table"
    },
    
    # C. Duplicate check on primary keys
    {
        "check_sql": """
            SELECT COUNT(*)
            FROM (
                SELECT user_id, COUNT(*)
                FROM users
                GROUP BY user_id
                HAVING COUNT(*) > 1
            );
        """,
        "expected": lambda result: result[0] == 0,
        "description": "No duplicate user_ids in users table"
    },
    {
        "check_sql": """
            SELECT COUNT(*)
            FROM (
                SELECT song_id, COUNT(*)
                FROM songs
                GROUP BY song_id
                HAVING COUNT(*) > 1
            );
        """,
        "expected": lambda result: result[0] == 0,
        "description": "No duplicate song_ids in songs table"
    },
    {
        "check_sql": """
            SELECT COUNT(*)
            FROM (
                SELECT artist_id, COUNT(*)
                FROM artists
                GROUP BY artist_id
                HAVING COUNT(*) > 1
            );
        """,
        "expected": lambda result: result[0] == 0,
        "description": "No duplicate artist_ids in artists table"
    },
    {
        "check_sql": """
            SELECT COUNT(*)
            FROM (
                SELECT start_time, COUNT(*)
                FROM time
                GROUP BY start_time
                HAVING COUNT(*) > 1
            );
        """,
        "expected": lambda result: result[0] == 0,
        "description": "No duplicate start_time in time table"
    },
    {
        "check_sql": """
            SELECT COUNT(*)
            FROM (
                SELECT songplay_id, COUNT(*)
                FROM songplays
                GROUP BY songplay_id
                HAVING COUNT(*) > 1
            );
        """,
        "expected": lambda result: result[0] == 0,
        "description": "No duplicate songplay_id in songplays table"
    }

]
