"""add_user_mixin_to_search_queries

Revision ID: de114c0980ed
Revises: 9f7826727a55
Create Date: 2025-09-17 09:26:13.235075

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'de114c0980ed'
down_revision = '9f7826727a55'
branch_labels = None
depends_on = None


def upgrade():
    # Add created_by_email and modified_by_email columns to search_queries table
    op.add_column('search_queries', sa.Column('created_by_email', sa.String(), nullable=True))
    op.add_column('search_queries', sa.Column('modified_by_email', sa.String(), nullable=True))


def downgrade():
    # Remove the columns
    op.drop_column('search_queries', 'modified_by_email')
    op.drop_column('search_queries', 'created_by_email')
