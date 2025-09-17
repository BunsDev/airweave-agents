"""Shared analytics utilities for search operations."""

from typing import Any, Dict, Optional

from airweave.analytics.service import analytics
from airweave.api.context import ApiContext


def build_search_properties(
    ctx: ApiContext,
    query: str,
    collection_slug: str,
    duration_ms: float,
    search_type: str = "regular",
    results: Optional[list] = None,
    response_type: Optional[str] = None,
    status: str = "success",
) -> Dict[str, Any]:
    """Build unified analytics properties for search operations.

    Args:
        ctx: API context with user and organization info
        query: Search query text
        collection_slug: Collection identifier
        duration_ms: Search duration in milliseconds
        search_type: Type of search ("regular" or "streaming")
        results: Search results list (optional)
        response_type: Response type (optional)
        status: Search status (default: "success")

    Returns:
        Dictionary of analytics properties
    """
    properties = {
        "query_length": len(query),
        "collection_slug": collection_slug,
        "duration_ms": duration_ms,
        "search_type": search_type,
        "organization_name": getattr(ctx.organization, "name", "unknown"),
        "status": status,
    }

    # Add response type if provided
    if response_type:
        properties["response_type"] = response_type

    # Add results count if results are provided
    if results:
        properties["results_count"] = len(results)

    return properties


def build_search_error_properties(
    query: str,
    collection_slug: str,
    duration_ms: float,
    error: Exception,
    search_type: str = "regular",
) -> Dict[str, Any]:
    """Build analytics properties for search errors.

    Args:
        query: Search query text
        collection_slug: Collection identifier
        duration_ms: Search duration in milliseconds
        error: The exception that occurred
        search_type: Type of search ("regular" or "streaming")

    Returns:
        Dictionary of analytics properties
    """
    return {
        "query_length": len(query) if query else 0,
        "collection_slug": collection_slug,
        "duration_ms": duration_ms,
        "search_type": search_type,
        "error": type(error).__name__,
    }


def track_search_event(
    ctx: ApiContext,
    properties: Dict[str, Any],
    event_name: str,
) -> None:
    """Track a search analytics event.

    Args:
        ctx: API context with user and organization info
        properties: Analytics properties dictionary
        event_name: Name of the event to track
    """
    analytics.track_event(
        event_name=event_name,
        distinct_id=str(ctx.user.id) if ctx.user else f"api_key_{ctx.organization.id}",
        properties=properties,
        groups={"organization": str(ctx.organization.id)},
    )


def build_search_persistence_properties(
    ctx: ApiContext,
    query: str,
    collection_slug: str,
    duration_ms: float,
    results_count: int,
    status: str,
    search_type: str = "regular",
    response_type: Optional[str] = None,
    query_expansion_enabled: Optional[bool] = None,
    reranking_enabled: Optional[bool] = None,
    query_interpretation_enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    """Build analytics properties for search persistence events.

    Args:
        ctx: API context with user and organization info
        query: Search query text
        collection_slug: Collection identifier
        duration_ms: Search duration in milliseconds
        results_count: Number of results returned
        status: Search status
        search_type: Type of search ("regular" or "streaming")
        response_type: Response type (optional)
        query_expansion_enabled: Whether query expansion was enabled
        reranking_enabled: Whether reranking was enabled
        query_interpretation_enabled: Whether query interpretation was enabled

    Returns:
        Dictionary of analytics properties
    """
    properties = {
        "query_length": len(query),
        "collection_slug": collection_slug,
        "duration_ms": duration_ms,
        "results_count": results_count,
        "search_type": search_type,
        "status": status,
        "organization_name": getattr(ctx.organization, "name", "unknown"),
    }

    # Add optional properties
    if response_type:
        properties["response_type"] = response_type
    if query_expansion_enabled is not None:
        properties["query_expansion_enabled"] = query_expansion_enabled
    if reranking_enabled is not None:
        properties["reranking_enabled"] = reranking_enabled
    if query_interpretation_enabled is not None:
        properties["query_interpretation_enabled"] = query_interpretation_enabled

    return properties


def track_search_persisted(
    ctx: ApiContext,
    query: str,
    collection_slug: str,
    duration_ms: float,
    results_count: int,
    status: str,
    search_type: str = "regular",
    response_type: Optional[str] = None,
    query_expansion_enabled: Optional[bool] = None,
    reranking_enabled: Optional[bool] = None,
    query_interpretation_enabled: Optional[bool] = None,
) -> None:
    """Track when a search is persisted to the database.

    Args:
        ctx: API context with user and organization info
        query: Search query text
        collection_slug: Collection identifier
        duration_ms: Search duration in milliseconds
        results_count: Number of results returned
        status: Search status
        search_type: Type of search ("regular" or "streaming")
        response_type: Response type (optional)
        query_expansion_enabled: Whether query expansion was enabled
        reranking_enabled: Whether reranking was enabled
        query_interpretation_enabled: Whether query interpretation was enabled
    """
    properties = build_search_persistence_properties(
        ctx=ctx,
        query=query,
        collection_slug=collection_slug,
        duration_ms=duration_ms,
        results_count=results_count,
        status=status,
        search_type=search_type,
        response_type=response_type,
        query_expansion_enabled=query_expansion_enabled,
        reranking_enabled=reranking_enabled,
        query_interpretation_enabled=query_interpretation_enabled,
    )

    track_search_event(ctx, properties, "search_persisted")


def track_search_evolution_detected(
    ctx: ApiContext,
    query: str,
    sophistication_score: float,
    previous_avg: float,
    improvement_percentage: float,
    collection_slug: str,
) -> None:
    """Track when search evolution is detected.

    Args:
        ctx: API context with user and organization info
        query: Search query text
        sophistication_score: Current query sophistication score
        previous_avg: Previous average sophistication score
        improvement_percentage: Percentage improvement
        collection_slug: Collection identifier
    """
    properties = {
        "query_text": query,
        "query_length": len(query),
        "collection_slug": collection_slug,
        "sophistication_score": sophistication_score,
        "previous_avg": previous_avg,
        "improvement_percentage": improvement_percentage,
        "organization_name": getattr(ctx.organization, "name", "unknown"),
    }

    track_search_event(ctx, properties, "search_evolution_detected")


def track_search_feature_adopted(
    ctx: ApiContext,
    feature: str,
    query: str,
    collection_slug: str,
    feature_details: Optional[Dict[str, Any]] = None,
) -> None:
    """Track when a user adopts a new search feature.

    Args:
        ctx: API context with user and organization info
        feature: Name of the feature adopted
        query: Search query text
        collection_slug: Collection identifier
        feature_details: Additional feature-specific details
    """
    properties = {
        "feature": feature,
        "query_text": query,
        "query_length": len(query),
        "collection_slug": collection_slug,
        "organization_name": getattr(ctx.organization, "name", "unknown"),
    }

    if feature_details:
        properties.update(feature_details)

    track_search_event(ctx, properties, "search_feature_adopted")
