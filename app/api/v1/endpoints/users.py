"""
User Management Endpoints for Admin
Provides CRUD operations for user accounts including password resets
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text, or_
from typing import List, Optional
from datetime import datetime

from app.db.session import get_db
from app.api.dependencies import require_role
from app.models.database import User, UserRole
from app.models.schemas import UserCreate, UserResponse, UserUpdate, PasswordReset
from app.utils.auth import get_password_hash
import re

router = APIRouter(prefix="/users", tags=["users"])


def validate_password(password: str) -> tuple[bool, str]:
    """
    Validate password meets requirements:
    - Minimum 8 characters
    - At least one non-alphanumeric character
    
    Returns: (is_valid, error_message)
    """
    if len(password) < 8:
        return False, "Password must be at least 8 characters long"
    
    if not any(not c.isalnum() for c in password):
        return False, "Password must contain at least one non-alphanumeric character (!@#$%^&* etc.)"
    
    return True, ""


@router.get("", response_model=List[UserResponse])
async def list_users(
    search: Optional[str] = None,
    role: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """
    List all users with optional filtering (ADMIN only)
    
    Query parameters:
    - search: Search by username, email, or full_name
    - role: Filter by role
    - is_active: Filter by active status
    """
    query = "SELECT id, username, email, full_name, role, is_active, created_at FROM app.users WHERE 1=1"
    params = {}
    
    if search:
        query += " AND (username LIKE :search OR email LIKE :search OR full_name LIKE :search)"
        params["search"] = f"%{search}%"
    
    if role:
        query += " AND role = :role"
        params["role"] = role
    
    if is_active is not None:
        query += " AND is_active = :is_active"
        params["is_active"] = is_active
    
    query += " ORDER BY full_name, username"
    
    results = db.execute(text(query), params).fetchall()
    
    return [
        UserResponse(
            id=r.id,
            username=r.username,
            email=r.email,
            full_name=r.full_name,
            role=UserRole(r.role.lower()) if isinstance(r.role, str) else r.role,
            is_active=bool(r.is_active),
            created_at=r.created_at
        )
        for r in results
    ]


@router.get("/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """Get user details by ID (ADMIN only)"""
    
    query = text("""
        SELECT id, username, email, full_name, role, is_active, created_at
        FROM app.users
        WHERE id = :user_id
    """)
    
    result = db.execute(query, {"user_id": user_id}).first()
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID {user_id} not found"
        )
    
    return UserResponse(
        id=result.id,
        username=result.username,
        email=result.email,
        full_name=result.full_name,
        role=UserRole(result.role),
        is_active=bool(result.is_active),
        created_at=result.created_at
    )


@router.post("", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    user_data: UserCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """
    Create a new user (ADMIN only)
    
    Password requirements:
    - Minimum 8 characters
    - At least one non-alphanumeric character
    """
    # Validate password
    is_valid, error_msg = validate_password(user_data.password)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_msg
        )
    
    # Check if username exists
    existing_username = db.execute(
        text("SELECT id FROM app.users WHERE username = :username"),
        {"username": user_data.username}
    ).first()
    
    if existing_username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already exists"
        )
    
    # Check if email exists
    existing_email = db.execute(
        text("SELECT id FROM app.users WHERE email = :email"),
        {"email": user_data.email}
    ).first()
    
    if existing_email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already exists"
        )
    
    # Create new user
    hashed_password = get_password_hash(user_data.password)
    
    insert_query = text("""
        INSERT INTO app.users (username, email, full_name, role, hashed_password, is_active, created_at)
        VALUES (:username, :email, :full_name, :role, :hashed_password, 1, GETDATE())
    """)
    
    db.execute(insert_query, {
        "username": user_data.username,
        "email": user_data.email,
        "full_name": user_data.full_name,
        "role": user_data.role.value,
        "hashed_password": hashed_password
    })
    db.commit()
    
    # Retrieve the created user
    new_user = db.execute(
        text("SELECT id, username, email, full_name, role, is_active, created_at FROM app.users WHERE username = :username"),
        {"username": user_data.username}
    ).first()
    
    return UserResponse(
        id=new_user.id,
        username=new_user.username,
        email=new_user.email,
        full_name=new_user.full_name,
        role=UserRole(new_user.role),
        is_active=bool(new_user.is_active),
        created_at=new_user.created_at
    )


@router.put("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """
    Update user information (ADMIN only)
    
    Can update: email, full_name, role, is_active
    Cannot update: username (primary identifier)
    """
    # Check if user exists
    existing_user = db.execute(
        text("SELECT id, username FROM app.users WHERE id = :user_id"),
        {"user_id": user_id}
    ).first()
    
    if not existing_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID {user_id} not found"
        )
    
    # Build dynamic update query
    update_fields = []
    params = {"user_id": user_id}
    
    if user_data.email is not None:
        # Check if email is already taken by another user
        email_check = db.execute(
            text("SELECT id FROM app.users WHERE email = :email AND id != :user_id"),
            {"email": user_data.email, "user_id": user_id}
        ).first()
        
        if email_check:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already in use by another user"
            )
        
        update_fields.append("email = :email")
        params["email"] = user_data.email
    
    if user_data.full_name is not None:
        update_fields.append("full_name = :full_name")
        params["full_name"] = user_data.full_name
    
    if user_data.role is not None:
        update_fields.append("role = :role")
        params["role"] = user_data.role.value
    
    if user_data.is_active is not None:
        update_fields.append("is_active = :is_active")
        params["is_active"] = user_data.is_active
    
    if not update_fields:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update"
        )
    
    # Execute update
    update_query = text(f"""
        UPDATE app.users 
        SET {', '.join(update_fields)}
        WHERE id = :user_id
    """)
    
    db.execute(update_query, params)
    db.commit()
    
    # Return updated user
    updated_user = db.execute(
        text("SELECT id, username, email, full_name, role, is_active, created_at FROM app.users WHERE id = :user_id"),
        {"user_id": user_id}
    ).first()
    
    return UserResponse(
        id=updated_user.id,
        username=updated_user.username,
        email=updated_user.email,
        full_name=updated_user.full_name,
        role=UserRole(updated_user.role),
        is_active=bool(updated_user.is_active),
        created_at=updated_user.created_at
    )


@router.post("/{user_id}/reset-password", response_model=dict)
async def reset_user_password(
    user_id: int,
    password_data: PasswordReset,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """
    Reset a user's password (ADMIN only)
    
    Password requirements:
    - Minimum 8 characters
    - At least one non-alphanumeric character
    """
    # Validate password
    is_valid, error_msg = validate_password(password_data.new_password)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_msg
        )
    
    # Check if user exists
    existing_user = db.execute(
        text("SELECT id, username FROM app.users WHERE id = :user_id"),
        {"user_id": user_id}
    ).first()
    
    if not existing_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID {user_id} not found"
        )
    
    # Hash and update password
    hashed_password = get_password_hash(password_data.new_password)
    
    update_query = text("""
        UPDATE app.users 
        SET hashed_password = :hashed_password
        WHERE id = :user_id
    """)
    
    db.execute(update_query, {
        "user_id": user_id,
        "hashed_password": hashed_password
    })
    db.commit()
    
    return {
        "success": True,
        "message": f"Password reset successfully for user '{existing_user.username}'",
        "user_id": user_id
    }


@router.delete("/{user_id}", response_model=dict)
async def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """
    Delete a user (ADMIN only)
    
    Note: This is a hard delete. Consider using is_active=false instead for soft deletes.
    """
    # Prevent deleting yourself
    if user_id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )
    
    # Check if user exists
    existing_user = db.execute(
        text("SELECT id, username FROM app.users WHERE id = :user_id"),
        {"user_id": user_id}
    ).first()
    
    if not existing_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with ID {user_id} not found"
        )
    
    # Delete user
    delete_query = text("DELETE FROM app.users WHERE id = :user_id")
    db.execute(delete_query, {"user_id": user_id})
    db.commit()
    
    return {
        "success": True,
        "message": f"User '{existing_user.username}' deleted successfully",
        "user_id": user_id
    }