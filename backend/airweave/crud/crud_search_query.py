"""CRUD operations for search query models."""

from typing import List, Optional
from uuid import UUID

from sqlalchemy import and_, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from airweave.api.context import ApiContext
from airweave.crud._base_organization import CRUDBaseOrganization
from airweave.models.search_query import SearchQuery
from airweave.schemas.search_query import SearchQueryCreate, SearchQueryUpdate


class CRUDSearchQuery(CRUDBaseOrganization[SearchQuery, SearchQueryCreate, SearchQueryUpdate]):
    """CRUD operations for search query persistence."""



    async def get_user_search_history(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        ctx: ApiContext,
        limit: int = 50,
        offset: int = 0,
    ) -> List[SearchQuery]:
        """Get search history for a specific user.

        Args:
            db: Database session
            user_id: ID of the user
            ctx: API context
            limit: Maximum number of results to return
            offset: Number of results to skip

        Returns:
            List of search queries for the user
        """
        query = (
            select(SearchQuery)
            .where(
                and_(
                    SearchQuery.organization_id == ctx.organization.id,
                    SearchQuery.user_id == user_id,
                )
            )
            .order_by(desc(SearchQuery.created_at))
            .offset(offset)
            .limit(limit)
        )

        result = await db.execute(query)
        return list(result.unique().scalars().all())

    async def get_collection_search_analytics(
        self,
        db: AsyncSession,
        *,
        collection_id: UUID,
        ctx: ApiContext,
        days: int = 30,
    ) -> dict:
        """Get search analytics for a specific collection.

        Args:
            db: Database session
            collection_id: ID of the collection
            ctx: API context
            days: Number of days to analyze

        Returns:
            Dictionary with analytics data
        """
        # Get total searches
        total_query = select(func.count(SearchQuery.id)).where(
            and_(
                SearchQuery.organization_id == ctx.organization.id,
                SearchQuery.collection_id == collection_id,
            )
        )
        total_result = await db.execute(total_query)
        total_searches = total_result.scalar() or 0

        # Get successful searches
        successful_query = select(func.count(SearchQuery.id)).where(
            and_(
                SearchQuery.organization_id == ctx.organization.id,
                SearchQuery.collection_id == collection_id,
                SearchQuery.status == "success",
            )
        )
        successful_result = await db.execute(successful_query)
        successful_searches = successful_result.scalar() or 0

        # Get average duration
        duration_query = select(func.avg(SearchQuery.duration_ms)).where(
            and_(
                SearchQuery.organization_id == ctx.organization.id,
                SearchQuery.collection_id == collection_id,
            )
        )
        duration_result = await db.execute(duration_query)
        avg_duration = duration_result.scalar() or 0

        # Get average results count
        results_query = select(func.avg(SearchQuery.results_count)).where(
            and_(
                SearchQuery.organization_id == ctx.organization.id,
                SearchQuery.collection_id == collection_id,
            )
        )
        results_result = await db.execute(results_query)
        avg_results = results_result.scalar() or 0

        return {
            "total_searches": total_searches,
            "successful_searches": successful_searches,
            "success_rate": (successful_searches / total_searches * 100)
            if total_searches > 0
            else 0,
            "average_duration_ms": float(avg_duration),
            "average_results_count": float(avg_results),
        }

    async def get_popular_queries(
        self,
        db: AsyncSession,
        *,
        ctx: ApiContext,
        collection_id: Optional[UUID] = None,
        limit: int = 10,
        days: int = 30,
    ) -> List[dict]:
        """Get most popular search queries.

        Args:
            db: Database session
            ctx: API context
            collection_id: Optional collection ID to filter by
            limit: Maximum number of queries to return
            days: Number of days to analyze

        Returns:
            List of popular queries with counts
        """
        query = (
            select(
                SearchQuery.query_text,
                func.count(SearchQuery.id).label("count"),
            )
            .where(SearchQuery.organization_id == ctx.organization.id)
            .group_by(SearchQuery.query_text)
            .order_by(desc("count"))
            .limit(limit)
        )

        if collection_id:
            query = query.where(SearchQuery.collection_id == collection_id)

        result = await db.execute(query)
        return [{"query_text": row.query_text, "count": row.count} for row in result.all()]

    async def get_search_evolution_data(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        ctx: ApiContext,
        days: int = 30,
    ) -> List[dict]:
        """Get search evolution data for a user.

        Args:
            db: Database session
            user_id: ID of the user
            ctx: API context
            days: Number of days to analyze

        Returns:
            List of search evolution data points
        """
        query = (
            select(
                SearchQuery.created_at,
                SearchQuery.query_length,
                SearchQuery.duration_ms,
                SearchQuery.results_count,
                SearchQuery.status,
                SearchQuery.query_expansion_enabled,
                SearchQuery.reranking_enabled,
            )
            .where(
                and_(
                    SearchQuery.organization_id == ctx.organization.id,
                    SearchQuery.user_id == user_id,
                )
            )
            .order_by(SearchQuery.created_at)
        )

        result = await db.execute(query)
        return [
            {
                "created_at": row.created_at.isoformat(),
                "query_length": row.query_length,
                "duration_ms": row.duration_ms,
                "results_count": row.results_count,
                "status": row.status,
                "query_expansion_enabled": row.query_expansion_enabled,
                "reranking_enabled": row.reranking_enabled,
            }
            for row in result.all()
        ]


# Create singleton instance
search_query = CRUDSearchQuery(SearchQuery)
