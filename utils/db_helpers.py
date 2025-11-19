"""Database helper functions for merchant onboarding"""

import os
import logging
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

logger = logging.getLogger(__name__)

# Database connection pool
_db_pool = None


def get_db_pool():
    """Get or create database connection pool"""
    global _db_pool
    if _db_pool is None:
        db_dsn = os.getenv("DB_DSN")
        if not db_dsn:
            raise ValueError("DB_DSN environment variable not set")
        
        try:
            _db_pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=db_dsn
            )
            logger.info("Database connection pool created")
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise
    
    return _db_pool


def get_connection():
    """Get a database connection from the pool"""
    pool = get_db_pool()
    return pool.getconn()


def return_connection(conn):
    """Return a connection to the pool"""
    pool = get_db_pool()
    pool.putconn(conn)


# ============================================================================
# MERCHANT FUNCTIONS
# ============================================================================

def get_merchant(merchant_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get merchant and verify it belongs to user
    
    Args:
        merchant_id: Merchant identifier
        user_id: User identifier (Firebase UID)
    
    Returns:
        Merchant dict or None if not found/not owned by user
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Merchants table in public schema
        query = """
            SELECT * FROM merchants
            WHERE merchant_id = %s AND user_id = %s
        """
        
        cursor.execute(query, (merchant_id, user_id))
        result = cursor.fetchone()
        
        cursor.close()
        return dict(result) if result else None
        
    except psycopg2.Error as e:
        logger.error(f"Database error getting merchant: {e}")
        return None
    except Exception as e:
        logger.error(f"Error getting merchant: {e}")
        return None
    finally:
        if conn:
            return_connection(conn)


def create_merchant(
    merchant_id: str,
    user_id: str,
    shop_name: str,
    shop_url: Optional[str] = None,
    bot_name: Optional[str] = "AI Assistant",
    **kwargs
) -> bool:
    """
    Create a new merchant record
    
    Args:
        merchant_id: Merchant identifier
        user_id: User identifier
        shop_name: Shop name
        shop_url: Shop URL (optional)
        bot_name: Bot name (optional)
        **kwargs: Additional merchant fields
    
    Returns:
        True if created successfully
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            INSERT INTO merchants (
                merchant_id, user_id, shop_name, shop_url, bot_name, 
                status, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, 'active', NOW(), NOW())
            ON CONFLICT (merchant_id) DO UPDATE
            SET shop_name = EXCLUDED.shop_name,
                shop_url = EXCLUDED.shop_url,
                bot_name = EXCLUDED.bot_name,
                updated_at = NOW()
        """
        
        cursor.execute(query, (merchant_id, user_id, shop_name, shop_url, bot_name))
        conn.commit()
        cursor.close()
        
        logger.info(f"Created/updated merchant: {merchant_id}")
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Database error creating merchant: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Error creating merchant: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)


# ============================================================================
# SUBSCRIPTION FUNCTIONS
# ============================================================================

def check_subscription(user_id: str) -> bool:
    """
    Check if user has active subscription
    
    Args:
        user_id: User identifier
    
    Returns:
        True if user has active subscription
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Check user_subscriptions in billing schema
        query = """
            SELECT subscription_id 
            FROM billing.user_subscriptions
            WHERE user_id = %s 
                AND status = 'active'
                AND current_period_end > NOW()
            LIMIT 1
        """
        
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()
        cursor.close()
        
        return result is not None
        
    except Exception as e:
        logger.error(f"Error checking subscription: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)


def get_subscription(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get user's active subscription details
    
    Args:
        user_id: User identifier
    
    Returns:
        Subscription dict or None
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get from billing.user_subscriptions
        query = """
            SELECT * 
            FROM billing.user_subscriptions
            WHERE user_id = %s 
                AND status = 'active'
                AND current_period_end > NOW()
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()
        cursor.close()
        
        return dict(result) if result else None
        
    except Exception as e:
        logger.error(f"Error getting subscription: {e}")
        return None
    finally:
        if conn:
            return_connection(conn)


# ============================================================================
# ONBOARDING JOB FUNCTIONS
# ============================================================================

def create_onboarding_job(
    job_id: str,
    merchant_id: str,
    user_id: str
) -> bool:
    """
    Create onboarding job record in database
    
    Args:
        job_id: Job identifier
        merchant_id: Merchant identifier
        user_id: User identifier
    
    Returns:
        True if created successfully
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            INSERT INTO onboarding_jobs (
                job_id, merchant_id, user_id, status, progress, created_at, updated_at
            )
            VALUES (%s, %s, %s, 'pending', 0, NOW(), NOW())
        """
        
        cursor.execute(query, (job_id, merchant_id, user_id))
        conn.commit()
        cursor.close()
        
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Database error creating job: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Error creating job: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)


def update_onboarding_job(
    job_id: str,
    status: str,
    progress: Optional[int] = None,
    current_step: Optional[str] = None,
    error_message: Optional[str] = None
) -> bool:
    """
    Update onboarding job status
    
    Args:
        job_id: Job identifier
        status: Job status
        progress: Progress percentage (0-100)
        current_step: Current step name
        error_message: Error message if failed
    
    Returns:
        True if updated successfully
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # TODO: Update based on your actual schema
        query = """
            UPDATE onboarding_jobs
            SET status = %s,
                progress = COALESCE(%s, progress),
                current_step = COALESCE(%s, current_step),
                error_message = %s,
                updated_at = NOW()
            WHERE job_id = %s
        """
        
        cursor.execute(query, (status, progress, current_step, error_message, job_id))
        conn.commit()
        cursor.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating job: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def verify_merchant_access(merchant_id: str, user_id: str) -> bool:
    """
    Verify that merchant belongs to user
    
    Args:
        merchant_id: Merchant identifier
        user_id: User identifier
    
    Returns:
        True if merchant belongs to user
    """
    merchant = get_merchant(merchant_id, user_id)
    return merchant is not None


def get_user_merchants(user_id: str) -> list:
    """
    Get all merchants for a user
    
    Args:
        user_id: User identifier
    
    Returns:
        List of merchant dicts
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT * FROM merchants
            WHERE user_id = %s
            ORDER BY created_at DESC
        """
        
        cursor.execute(query, (user_id,))
        results = cursor.fetchall()
        cursor.close()
        
        return [dict(row) for row in results]
        
    except Exception as e:
        logger.error(f"Error getting user merchants: {e}")
        return []
    finally:
        if conn:
            return_connection(conn)

