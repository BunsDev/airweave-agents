"""Api key model."""

from datetime import datetime
from typing import TYPE_CHECKING, List

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from airweave.models._base import OrganizationBase, UserMixin

if TYPE_CHECKING:
    from airweave.models.search_query import SearchQuery


class APIKey(OrganizationBase, UserMixin):
    """SQLAlchemy model for the APIKey table."""

    __tablename__ = "api_key"

    encrypted_key: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    expiration_date: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)

    # Search queries performed with this API key
    search_queries: Mapped[List["SearchQuery"]] = relationship(
        "SearchQuery", back_populates="api_key", lazy="noload"
    )
