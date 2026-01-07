from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import timedelta
from app.db.session import get_db
from app.models.database import User, UserRole
from app.models.schemas import UserCreate, UserResponse, Token, UserUpdate
from app.utils.auth import verify_password, get_password_hash, create_access_token
from app.api.dependencies import get_current_active_user, require_role
from app.config import settings

router = APIRouter()


@router.post("/login", response_model=Token)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    """
    Authenticate user and return access token
    Now uses pt.employees table
    """
    # Query pt.employees table
    query = text("""
        SELECT employee_id, username, password_hash, is_active, role
        FROM pt.employees
        WHERE username = :username
    """)
    
    user = db.execute(query, {"username": form_data.username}).first()
    
    if not user or not user.password_hash or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    
    access_token_expires = timedelta(minutes=settings.access_token_expire_minutes)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: User = Depends(get_current_active_user)
):
    """
    Get current user information
    """
    return UserResponse(
        id=current_user.employee_id,
        username=current_user.username,
        email=current_user.email,
        first_name=current_user.first_name,
        last_name=current_user.last_name,
        role=UserRole(current_user.role.lower()) if isinstance(current_user.role, str) else UserRole(current_user.role),
        is_active=current_user.is_active,
        created_at=current_user.created_at
    )


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register_user(
    user_data: UserCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_role([UserRole.ADMIN]))
):
    """
    Register a new user (Admin only)
    Creates user in pt.employees table
    """
    # Check if username exists
    existing = db.execute(
        text("SELECT employee_id FROM pt.employees WHERE username = :username"),
        {"username": user_data.username}
    ).first()
    
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )
    
    # Check if email exists
    if user_data.email:
        existing_email = db.execute(
            text("SELECT employee_id FROM pt.employees WHERE email = :email"),
            {"email": user_data.email}
        ).first()
        
        if existing_email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
    
    # Create new user
    hashed_password = get_password_hash(user_data.password)
    
    insert_query = text("""
        INSERT INTO pt.employees 
        (username, email, first_name, last_name, role, password_hash, is_active, created_at, created_by)
        VALUES 
        (:username, :email, :first_name, :last_name, :role, :password_hash, 1, GETUTCDATE(), :created_by)
    """)
    
    db.execute(insert_query, {
        "username": user_data.username,
        "email": user_data.email,
        "first_name": user_data.first_name,
        "last_name": user_data.last_name,
        "role": user_data.role.value,
        "password_hash": hashed_password,
        "created_by": current_user.employee_id
    })
    db.commit()
    
    # Retrieve the created user
    new_user = db.execute(
        text("""
            SELECT employee_id, username, email, first_name, last_name, role, is_active, created_at 
            FROM pt.employees 
            WHERE username = :username
        """),
        {"username": user_data.username}
    ).first()
    
    return UserResponse(
        id=new_user.employee_id,
        username=new_user.username,
        email=new_user.email,
        first_name=new_user.first_name,
        last_name=new_user.last_name,
        role=UserRole(new_user.role.lower()) if isinstance(new_user.role, str) else new_user.role,
        is_active=bool(new_user.is_active),
        created_at=new_user.created_at
    )

