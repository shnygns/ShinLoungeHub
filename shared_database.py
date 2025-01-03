import sqlite3
import time
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
            # self.cur = self.connection.cursor()

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
        """Create database tables if they do not exist already."""
        schema_queries = [
            """
            CREATE TABLE IF NOT EXISTS lounges (
                id INTEGER PRIMARY KEY,
                name TEXT,
                bot_token TEXT UNIQUE NOT NULL,
                status INTEGER NOT NULL CHECK (status IN (0, 1)),
                active_user_count INTEGER DEFAULT 0,
                last_updated TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT,
                username TEXT,
                current_active_lounge STRING,
                last_seen TIMESTAMP,
                universal_ban BOOLEAN DEFAULT FALSE,
                FOREIGN KEY(current_active_lounge) REFERENCES lounges(bot_token)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS settings (
                setting TEXT PRIMARY KEY,
                value VARCHAR(255)
            )
            """
        ]
        with self.lock:
            cur = None
            try:
                cur = self.connection.cursor()  # Explicitly create the cursor
                for query in schema_queries:
                    cur.execute(query)
                self.connection.commit()  # Commit the changes
            except sqlite3.Error as e:
                logging.error(f"Error while ensuring schema: {e}")
                raise e
            finally:
                if cur:
                    cur.close()  # Ensure the cursor is closed even if an error occurs


    def _close(self):
        """close sqlite3 connection"""
        try:
            with self.lock:
                if not self.connection.closed:
                    self.connection.close()
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")


    def _execute(self, query, params=None, retries=10, delay=1):
        """Execute a query with retry mechanism for handling database locks."""
        for attempt in range(retries):
            try:
                with self.lock:
                    cur = self.connection.cursor()  # Create a new cursor for this operation
                    if params is None:
                        cur.execute(query)
                    else:
                        cur.execute(query, params)
                    self.connection.commit()  # Ensure changes are committed
                    return cur.fetchall() # Return results if applicable
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    logging.debug(f"Database is locked, retrying in {delay} seconds... (attempt {attempt + 1}/{retries})")
                    time.sleep(delay)
                else:
                    logging.error(f"Database error during execute: {e} - Query: {query} - Params: {params}")
                    raise Exception(f"Database operation failed: {e}")
        raise Exception(f"Database operation failed after {retries} retries: database is locked")


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
        try:
            self._execute(query, params)  # No need to check success; exceptions handle failure
            return True
        except Exception as e:
            logging.error(f"Error setting inactive lounges: {e}")
            return False
        

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
        try:
            self._execute(query)  # No need to check success; exceptions handle failure
            return True
        except Exception as e:
            logging.error(f"Error setting inactive lounges: {e}")
            return False
    


    def _record_lounge(self, name, bot_token, status) -> bool:
        """record lounge in database"""
        query = """
                INSERT INTO lounges (name, bot_token, status, last_updated)
                VALUES (?, ?, ?, ?)
                """
        params =  (name, bot_token, status, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"))
        try:
            self._execute(query, params)  # No need to check success; exceptions handle failure
            return True
        except Exception as e:
            logging.error(f"Error setting inactive lounges: {e}")
            return False
    
    #ping lounge using bot token
    def _lounge_activity_update(self, bot_token) -> bool:
        """ping lounge to update last_updated timestamp"""
        query = """
                UPDATE lounges
                SET status = 1, last_updated = ?
                WHERE bot_token = ?
                """
        params = (datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"), bot_token)
        try:
            self._execute(query, params)  # No need to check success; exceptions handle failure
            return True
        except Exception as e:
            logging.error(f"Error setting inactive lounges: {e}")
            return False
    

        # If lounge is already represented by bot token, update last active, or else record lounge
    def _record_lounge_or_ping(self, name, bot_token) -> bool:
        """Record lounge in database or update its activity."""
        query = """
                SELECT * FROM lounges
                WHERE bot_token = ?
                """
        params = (bot_token,)
        try:
            result = self._execute(query, params)
            lounge = result[0] if result else None
            if lounge:
                return self._lounge_activity_update(bot_token)
            else:
                return self._record_lounge(name, bot_token, status=LoungeStatus.ACTIVE.value)
        except Exception as e:
            logging.error(f"Error recording or pinging lounge: {e}")
            return False
        

    def update_user(self, user_id, full_name, username, lounge_name, bot_token, currently_joined=True) -> bool:
        """Add or update user in the database using bot token."""
        if self.is_user_banned(user_id):
            return False

        query = """
                SELECT id FROM lounges
                WHERE bot_token = ?
                """
        params = (bot_token,)
        try:
            result = self._execute(query, params)
            lounge_id = result[0]["id"] if result else None

            if lounge_id is None:
                self._record_lounge(lounge_name, bot_token, status=LoungeStatus.ACTIVE.value)
                lounge_id = self.get_lounge(bot_token)["id"]
            else:
                current_lounge = self.get_lounge(bot_token)
                current_lounge_name = current_lounge["name"] if current_lounge else None
                if current_lounge_name != lounge_name:
                    query = """
                            UPDATE lounges
                            SET name = ?
                            WHERE bot_token = ?
                            """
                    params = (lounge_name, bot_token)
                    self._execute(query, params)

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
            self._execute(query, params)
            self._lounge_activity_update(bot_token)
            return True
        except Exception as e:
            logging.error(f"Error updating user: {e}")
            return False
       

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
        try:
            self._execute(query, params)  # No need to check success; exceptions handle failure
            return True
        except Exception as e:
            logging.error(f"Error setting inactive lounges: {e}")
            return False
    
    
    def get_active_lounges(self) -> List[dict]:
        """Return all active lounges in the database."""
        query = """
                SELECT * FROM lounges
                WHERE status = 1
                """
        try:
            result = self._execute(query)
            lounges = [dict(lounge) for lounge in result] if result else []
            return lounges
        except Exception as e:
            logging.error(f"Error retrieving active lounges: {e}")
            raise e

        
    def get_active_users(self) -> List[dict]:
        """return all active users in a lounge"""
        query = """
                SELECT * FROM users
                WHERE current_active_lounge IS NOT NULL
                """
        try:
            result = self._execute(query)
            users = [dict(user) for user in result] if result else []
            return users
        except:
            raise Exception("Error returning all active users")
        
        
    def get_lounge_active_user_count(self, bot_token) -> int:
        """Return the active user count for a lounge."""
        query = """
                SELECT active_user_count FROM lounges
                WHERE bot_token = ?
                """
        params = (bot_token,)
        try:
            result = self._execute(query, params)
            return result[0]["active_user_count"] if result else 0
        except Exception as e:
            logging.error(f"Error retrieving active user count: {e}")
            raise Exception("Error returning active user count for a lounge")


    def get_lounge(self, bot_token) -> dict:
        """Return lounge from database."""
        query = """
                SELECT * FROM lounges
                WHERE bot_token = ?
                """
        params = (bot_token,)
        try:
            result = self._execute(query, params)
            return dict(result[0]) if result else None
        except Exception as e:
            logging.error(f"Error returning lounge from database: {e}")
            raise Exception("Error returning lounge from database")
        

    def universal_ban_user(self, user_id) -> bool:
        """record banned user in database"""
        query = """
                UPDATE users
                SET universal_ban = TRUE
                WHERE user_id = ?
                """
        params = (user_id,)
        try:
            self._execute(query, params)  # No need to check success; exceptions handle failure
            return True
        except Exception as e:
            logging.error(f"Error setting inactive lounges: {e}")
            return False
    

    def get_list_of_banned_users(self) -> List[int]:
        """return list of banned users"""
        query = """
                SELECT user_id FROM users
                WHERE universal_ban = TRUE
                """
        try:
            result= self._execute(query)
            return [user[0] for user in result] if result else []
        except:
            raise Exception("Error returning list of banned users")
        
    
    def is_user_banned(self, user_id) -> bool:
        """check if user is banned"""
        query = """
                SELECT universal_ban FROM users
                WHERE user_id = ?
                """
        params = (user_id,)
        try:
            result = self._execute(query, params)
            return result[0]['universal_ban'] if result else False
        except:
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
        try:
            self._execute(query, params)  # No need to check success; exceptions handle failure
            return True
        except Exception as e:
            logging.error(f"Error setting inactive lounges: {e}")
            return False
    

    def dewhitelist_user(self, user_id) -> bool:
        query = """
                UPDATE users
                SET current_active_lounge = NULL
                WHERE user_id = ?
                """
        params = (user_id,)
        try:
            self._execute(query, params)  # No need to check success; exceptions handle failure
            return True
        except Exception as e:
            logging.error(f"Error setting inactive lounges: {e}")
            return False
    

    # Return user's currently active lounge
    def get_user_current_lounge(self, user_id) -> str:
        query = """
                SELECT current_active_lounge FROM users
                WHERE user_id = ?
                """
        params = (user_id,)
        try:
            result = self._execute(query, params)
            return result[0]['current_active_lounge'] if result else None
        except:
            raise Exception("Error getting user's current lounge")
        
    
    #return lounges.name of the current_active_lounge for a given user id
    def get_user_current_lounge_name(self, user_id) -> str:
        query = """
                SELECT lounges.name FROM users
                JOIN lounges ON users.current_active_lounge = lounges.bot_token
                WHERE user_id = ?
                """
        params = (user_id,)
        try:
            result = self._execute(query, params)
            return result[0][0] if result else None
        except:
            raise Exception("Error getting user's current lounge name")


    async def timed_updates(self, context):
        """run timed updates"""
        try:
            self._set_inactive_lounges()
            self._update_active_user_count()
        except Exception as e:
            logging.error(f"Error running timed updates: {e}")
