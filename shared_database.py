import sqlite3
import logging
from  threading import RLock
from datetime import datetime, timedelta, timezone
from typing import List, Tuple
from enum import Enum


class LoungeStatus(Enum):
    ACTIVE = 1
    INACTIVE = 0


class SharedDatabase(object):

    DB_LOCATION = "../Shinlounge_hub.db"

    def __init__(self):
        """Initialize db class variables"""
        try:
            self.lock = RLock()
            self.connection = sqlite3.connect(SharedDatabase.DB_LOCATION, check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            self.cur = self.connection.cursor()

            self._ensure_schema()
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")
            raise e # Initialization errors are catastrophic and should be reraised

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        if exc_value is not None:
            # If there's an exception, roll back any changes made during the transaction.
            try:
                self.connection.rollback()
                logging.info(f"Database transaction rolled back due to an exception: {exc_value}")
            except sqlite3.Error as e:
                logging.error(f"Database transaction rolled back due to an exception: {exc_value} \n \
                              The following exception occured during rollback: {e}")
        else:
            # If no exception occurred, commit the changes.
            try:
                self.connection.commit()
                logging.info("Database transaction committed successfully.")
            except sqlite3.Error as e:
                logging.error(f"Database error during commit: {e}")

        # Attempt to close the connection
        self._close()


    def _ensure_schema(self):
        """create a database table if it does not exist already"""
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS lounges (
                id INTEGER PRIMARY KEY,
                name TEXT,
                bot_token TEXT UNIQUE NOT NULL,
                status INTEGER NOT NULL CHECK (status IN (0, 1)),
                active_user_count INTEGER DEFAULT 0,
                last_updated TIMESTAMP
            )
        """
        )

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT,
                username TEXT,
                current_active_lounge STRING,
                last_seen TIMESTAMP,
                universal_ban BOOLEAN DEFAULT FALSE,
                FOREIGN KEY(current_active_lounge) REFERENCES lounges(bot_token)
            )
        """
        )


        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                setting TEXT PRIMARY KEY,
                value VARCHAR(255)
            )
        """
        )

        self._commit()


    def _close(self):
        """close sqlite3 connection"""
        try:
            with self.lock:
                if not self.connection.closed:
                    self.connection.close()
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")


    def _execute(self, query, params=None):
        """Execute a row of data to current cursor with detailed error handling."""
        try:
            with self.lock:
                if params is None:
                    self.cur.execute(query)
                else:
                    self.cur.execute(query, params)
                return True
        except sqlite3.Error as e:
            logging.error(f"Database error during execute: {e} - Query: {query}")
            # You could optionally include more information about the error
            error_info = {
                'error': str(e),
                'query': query,
                'params': params
            }
            # Consider throwing a custom exception or return error information
            raise Exception(f"Database operation failed: {error_info}")
            # Alternatively, you can return False and error details
            # return False, error_info


    def _commit(self):
        """commit changes to database"""
        with self.lock:
            self.connection.commit()


    #Loop through all lounges and set to inactive if last updated is more than 10 minutes ago
    def _set_inactive_lounges(self) -> bool:
        """set all lounges with last_updated older than 10 minutes to inactive"""
        query = """
                UPDATE lounges
                SET status = 0
                WHERE last_updated < ?
                """
        params = (datetime.now(timezone.utc) - timedelta(days=1),)
        success = self._execute(query, params)
        if success:
            self._commit()
        else:
            raise Exception("Error setting inactive lounges")
        return success
    

    #Loop through all lounges and update the active_user_count to the number of users who have an active status in the users table
    def _update_active_user_count(self) -> bool:
        """update active_user_count for all lounges"""
        query = """
                UPDATE lounges
                SET active_user_count = (
                    SELECT COUNT(*) FROM users
                    WHERE current_active_lounge = lounges.bot_token
                    )
                """
        success = self._execute(query)
        if success:
            self._commit()
        else:
            raise Exception("Error updating active user count")
        return success  
    


    def _record_lounge(self, name, bot_token, status) -> bool:
        """record lounge in database"""
        query = """
                INSERT INTO lounges (name, bot_token, status, last_updated)
                VALUES (?, ?, ?, ?)
                """
        params =  (name, bot_token, status, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"))
        success = self._execute(query, params)
        if success:
            self._commit()
        else:
            raise Exception("Error recording lounge")
        return success
    
    #ping lounge using bot token
    def _lounge_activity_update(self, bot_token) -> bool:
        """ping lounge to update last_updated timestamp"""
        query = """
                UPDATE lounges
                SET status = 1, last_updated = ?
                WHERE bot_token = ?
                """
        params = (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"), bot_token)
        success = self._execute(query, params)
        if success:
            self._commit()
        else:
            raise Exception("Error pinging lounge")
        return success
    

        # If lounge is already represented by bot token, update last active, or else record lounge
    def _record_lounge_or_ping(self, name, bot_token) -> bool:
        """record lounge in database"""
        query = """
                SELECT * FROM lounges
                WHERE bot_token = ?
                """
        params = (bot_token,)
        success = self._execute(query, params)
        if success:
            lounge = self.cur.fetchone()
            if lounge:
                return self._lounge_activity_update(bot_token)
            else:
                return self._record_lounge(name, bot_token, status=LoungeStatus.ACTIVE.value)
        else:
            raise Exception("Error recording lounge or pinging lounge")
        

    def update_user(self, user_id, full_name, username, lounge_name, bot_token, currently_joined=True) -> bool:
        """add or update user in database using bot token"""
        if self.is_user_banned(user_id):
            return False
        query = """
                SELECT id FROM lounges
                WHERE bot_token = ?
                """
        params = (bot_token,)
        success = self._execute(query, params)
        if success:
            lounge_id = self.cur.fetchone()
            if lounge_id is None:
                self._record_lounge(lounge_name, bot_token, status=LoungeStatus.ACTIVE.value)
                lounge_id = self.get_lounge(bot_token)["id"]
            else:
                current_lounge_name = self.get_lounge(bot_token)["name"]
                if current_lounge_name != lounge_name:
                    query = """
                            UPDATE lounges
                            SET name = ?
                            WHERE bot_token = ?
                            """
                    params = (lounge_name, bot_token)
                    success = self._execute(query, params)
                    if success:
                        self._commit()
                    else:
                        raise Exception("Error updating lounge name")
                query = """
                    INSERT INTO users (user_id, full_name, username, current_active_lounge, last_seen)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        full_name=excluded.full_name,
                        username=excluded.username,
                        current_active_lounge=CASE 
                            WHEN users.current_active_lounge IS NULL 
                            THEN excluded.current_active_lounge 
                            ELSE users.current_active_lounge 
                        END,
                        last_seen=excluded.last_seen
                    """
                params = (user_id, full_name, username, bot_token, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"))
                success = self._execute(query, params)
                if success:
                    self._commit()
                    self._lounge_activity_update(bot_token)
                    return success

        else:
            raise Exception("Error updating user")


    def user_left_chat(self, user_id) -> bool:
        # Set the user's current_active_lounge to NULL
        query = """
                UPDATE users
                SET current_active_lounge = CASE
                    WHEN current_active_lounge != '*' THEN NULL
                    ELSE current_active_lounge
                END
                WHERE user_id = ?
                """
        params = (user_id,)
        success = self._execute(query, params)
        if success:
            self._commit()
        else:
            raise Exception("Error updating user")
        return success
    
    
    def get_active_lounges(self) -> List[dict]:
        """return all active lounges in database"""
        query = """
                SELECT * FROM lounges
                WHERE status = 1
                """

        success = self._execute(query)
        if success:
            #combine table column names with fatchall results to make a list of dictionaries
            lounges = [dict(lounge) for lounge in self.cur.fetchall()]
            return lounges
        else:
            raise Exception("Error returning all active lounges")
        
    def get_active_users(self) -> List[dict]:
        """return all active users in a lounge"""
        query = """
                SELECT * FROM users
                WHERE current_active_lounge IS NOT NULL
                """
        success = self._execute(query)
        if success:
            #combine table column names with fatchall results to make a list of dictionaries
            users = [dict(user) for user in self.cur.fetchall()]
            return users
        else:
            raise Exception("Error returning all active users")
        

    def get_lounge_active_user_count(self, bot_token) -> int:
        """return active user count for a lounge"""
        query = """
                SELECT active_user_count FROM lounges
                WHERE bot_token = ?
                """
        params = (bot_token,)
        success = self._execute(query, params)
        if success:
            active_user_count = self.cur.fetchone()
            return active_user_count[0] if active_user_count else 0
        else:
            raise Exception("Error returning active user count")


    def get_lounge(self, bot_token) -> dict:
        """return lounge from database"""
        query = """
                SELECT * FROM lounges
                WHERE bot_token = ?
                """
        params = (bot_token,)
        success = self._execute(query, params)
        if success:
            self.cur.row_factory = sqlite3.Row  # Set the row factory to sqlite3.Row
            lounge = self.cur.fetchone()
            return dict(lounge) if lounge else None
        else:
            raise Exception("Error returning lounge")
        

    def universal_ban_user(self, user_id) -> bool:
        """record banned user in database"""
        query = """
                UPDATE users
                SET universal_ban = TRUE
                WHERE user_id = ?
                """
        params = (user_id,)
        success = self._execute(query, params)
        if success:
            self._commit()
        else:
            raise Exception("Error recording banned user")
        return success
    
    def get_list_of_banned_users(self) -> List[int]:
        """return list of banned users"""
        query = """
                SELECT user_id FROM users
                WHERE universal_ban = TRUE
                """
        success = self._execute(query)
        if success:
            banned_users = self.cur.fetchall()
            return [user[0] for user in banned_users]
        else:
            raise Exception("Error returning list of banned users")
        
    
    def is_user_banned(self, user_id) -> bool:
        """check if user is banned"""
        query = """
                SELECT universal_ban FROM users
                WHERE user_id = ?
                """
        params = (user_id,)
        success = self._execute(query, params)
        if success:
            banned = self.cur.fetchone()
            return banned[0] if banned else False
        else:
            raise Exception("Error checking if user is banned")
    

    def ping(self, name, bot_token) -> bool:
        try:
            self._record_lounge_or_ping(name, bot_token)
            return True
        except Exception as e:
            logging.error(f"Error pinging lounge: {e}")
            return False
        
    # Whitelist user by setting current_active_lounge to "*" in the users table
    def whitelist_user(self, user_id) -> bool:
        query = """
                UPDATE users
                SET current_active_lounge = "*"
                WHERE user_id = ?
                """
        params = (user_id,)
        success = self._execute(query, params)
        if success:
            self._commit()
        else:
            raise Exception("Error whitelisting user")
        return success
    

    def dewhitelist_user(self, user_id) -> bool:
        query = """
                UPDATE users
                SET current_active_lounge = NULL
                WHERE user_id = ?
                """
        params = (user_id,)
        success = self._execute(query, params)
        if success:
            self._commit()
        else:
            raise Exception("Error dewhitelisting user")
        return success
    

    # Return user's currently active lounge
    def get_user_current_lounge(self, user_id) -> str:
        query = """
                SELECT current_active_lounge FROM users
                WHERE user_id = ?
                """
        params = (user_id,)
        success = self._execute(query, params)
        if success:
            current_lounge = self.cur.fetchone()
            return current_lounge[0] if current_lounge else None
        else:
            raise Exception("Error getting user's current lounge")
        
    
    #return lounges.name of the current_active_lounge for a given user id
    def get_user_current_lounge_name(self, user_id) -> str:
        query = """
                SELECT lounges.name FROM users
                JOIN lounges ON users.current_active_lounge = lounges.bot_token
                WHERE user_id = ?
                """
        params = (user_id,)
        success = self._execute(query, params)
        if success:
            current_lounge_name = self.cur.fetchone()
            return current_lounge_name[0] if current_lounge_name else None
        else:
            raise Exception("Error getting user's current lounge name")


    async def timed_updates(self, context):
        """run timed updates"""
        try:
            self._set_inactive_lounges()
            self._update_active_user_count()
        except Exception as e:
            logging.error(f"Error running timed updates: {e}")
