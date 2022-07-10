"""add a date founded column

Revision ID: 887cfd977f50
Revises: 
Create Date: 2022-07-05 11:36:21.530264

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Column, DateTime


# revision identifiers, used by Alembic.
revision = '887cfd977f50'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('NBA_TEAMS',
    Column('date_founded', DateTime())
    )


def downgrade():
    op.drop_column('NBA_TEAMS','date_founded')
