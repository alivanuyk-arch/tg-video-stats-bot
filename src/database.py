# src/database.py
import asyncpg
import logging
from typing import List, Dict, Any
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.pool = None
    
    async def connect(self):
        """Создание пула соединений"""
        self.pool = await asyncpg.create_pool(
            self.connection_string,
            min_size=1,
            max_size=10,
            command_timeout=60
        )
        logger.info("Database connection pool created")
    
    async def disconnect(self):
        """Закрытие пула соединений"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Контекстный менеджер для соединения"""
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as connection:
            yield connection
    
    async def execute_query(self, sql: str, params: list = None) -> List[Dict]:
        """Выполнение SQL запроса с возвратом результата"""
        async with self.get_connection() as conn:
            try:
                if params:
                    rows = await conn.fetch(sql, *params)
                else:
                    rows = await conn.fetch(sql)
                
                # Преобразуем в список словарей
                return [dict(row) for row in rows]
            except Exception as e:
                logger.error(f"Query execution error: {e}\nQuery: {sql}")
                raise
    
    async def execute_scalar(self, sql: str, params: list = None) -> Any:
        """Выполнение запроса с возвратом одного значения"""
        async with self.get_connection() as conn:
            try:
                if params:
                    result = await conn.fetchval(sql, *params)
                else:
                    result = await conn.fetchval(sql)
                return result
            except Exception as e:
                logger.error(f"Scalar query error: {e}\nQuery: {sql}")
                raise
    
    async def check_connection(self) -> bool:
        """Проверка соединения с базой"""
        try:
            async with self.get_connection() as conn:
                await conn.fetchval("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"Database connection check failed: {e}")
            return False