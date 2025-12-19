from __future__ import annotations

import uuid
from datetime import datetime
from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from sqlalchemy.types import UnicodeText

from app.db.base import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UNIQUEIDENTIFIER(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.sysdatetimeoffset(), nullable=False
    )

    profiles: Mapped[list["Profile"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    runs: Mapped[list["Run"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class Profile(Base):
    __tablename__ = "profiles"

    id: Mapped[uuid.UUID] = mapped_column(
        UNIQUEIDENTIFIER(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UNIQUEIDENTIFIER(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    profile_key: Mapped[str] = mapped_column(String(64), nullable=False)
    focus_config_json: Mapped[str] = mapped_column(UnicodeText(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.sysdatetimeoffset(), nullable=False
    )

    user: Mapped["User"] = relationship(back_populates="profiles")
    runs: Mapped[list["Run"]] = relationship(
        back_populates="profile", cascade="all, delete-orphan"
    )
    url_pool: Mapped[list["UrlPoolEntry"]] = relationship(
        back_populates="profile", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("user_id", "profile_key", name="uq_profiles_user_profilekey"),
        Index("ix_profiles_user_profilekey", "user_id", "profile_key"),
    )


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UNIQUEIDENTIFIER(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UNIQUEIDENTIFIER(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True
    )
    profile_id: Mapped[uuid.UUID] = mapped_column(
        UNIQUEIDENTIFIER(as_uuid=True),
        ForeignKey("profiles.id"),
        nullable=False,
        index=True,
    )

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="queued")
    params_json: Mapped[str | None] = mapped_column(UnicodeText(), nullable=True)
    summary_path: Mapped[str | None] = mapped_column(String(512), nullable=True)

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.sysdatetimeoffset(), nullable=False
    )

    user: Mapped["User"] = relationship(back_populates="runs")
    profile: Mapped["Profile"] = relationship(back_populates="runs")
    items: Mapped[list["RunItem"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class RunItem(Base):
    __tablename__ = "run_items"

    id: Mapped[uuid.UUID] = mapped_column(
        UNIQUEIDENTIFIER(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        UNIQUEIDENTIFIER(as_uuid=True),
        ForeignKey("runs.id"),
        nullable=False,
        index=True,
    )
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    url_hash: Mapped[str] = mapped_column(String(40), nullable=False)
    job_source: Mapped[str] = mapped_column(String(32), nullable=False, default="stepstone")
    job_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    final_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    llm_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bucket: Mapped[str | None] = mapped_column(String(64), nullable=True)
    output_dir: Mapped[str | None] = mapped_column(String(512), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.sysdatetimeoffset(), nullable=False
    )

    run: Mapped["Run"] = relationship(back_populates="items")

    __table_args__ = (
        UniqueConstraint("run_id", "url_hash", name="uq_run_items_run_urlhash"),
        Index("ix_run_items_run_urlhash", "run_id", "url_hash"),
    )


class UrlPoolEntry(Base):
    __tablename__ = "url_pool"

    id: Mapped[uuid.UUID] = mapped_column(
        UNIQUEIDENTIFIER(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    profile_id: Mapped[uuid.UUID] = mapped_column(
        UNIQUEIDENTIFIER(as_uuid=True),
        ForeignKey("profiles.id"),
        nullable=False,
        index=True,
    )
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    url_hash: Mapped[str] = mapped_column(String(40), nullable=False)

    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.sysdatetimeoffset(), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.sysdatetimeoffset(), nullable=False
    )

    profile: Mapped["Profile"] = relationship(back_populates="url_pool")

    __table_args__ = (
        UniqueConstraint("profile_id", "url_hash", name="uq_url_pool_profile_urlhash"),
        Index("ix_url_pool_profile_urlhash", "profile_id", "url_hash"),
    )
