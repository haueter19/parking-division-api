from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.models.database import UserRole
from app.utils.auth import decode_access_token

# OAuth2 scheme for token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/auth/login")


class UserProxy:
    """
    Simple proxy object to hold user data without ORM relationships
    Avoids SQLAlchemy relationship conflicts
    """
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    @property
    def id(self):
        """Alias for employee_id"""
        return self.employee_id
    
    @property
    def hashed_password(self):
        """Alias for password_hash"""
        return self.password_hash
    
    @property
    def full_name(self):
        """Computed full name from first_name and last_name"""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        elif self.first_name:
            return self.first_name
        elif self.last_name:
            return self.last_name
        return None
    

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> UserProxy:
    """
    Dependency to get the current authenticated user from JWT token
    Now queries pt.employees table using raw SQL
    
    Raises:
        HTTPException: If token is invalid or user not found
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    username = decode_access_token(token)
    if username is None:
        raise credentials_exception
    
    # Query pt.employees table using raw SQL
    query = text("""
        SELECT employee_id, username, email, first_name, last_name, 
               role, password_hash, is_active, created_at
        FROM pt.employees
        WHERE username = :username
    """)
    
    result = db.execute(query, {"username": username}).first()
    
    if result is None:
        raise credentials_exception
    
    # Create a UserProxy object from the query result
    user = UserProxy(
        employee_id=result.employee_id,
        username=result.username,
        email=result.email,
        first_name=result.first_name,
        last_name=result.last_name,
        role=result.role,
        password_hash=result.password_hash,
        is_active=result.is_active,
        created_at=result.created_at
    )
    
    return user


async def get_current_active_user(
    current_user: UserProxy = Depends(get_current_user)
) -> UserProxy:
    """
    Dependency to get current active user
    
    Raises:
        HTTPException: If user is inactive
    """
    if not current_user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


def require_role(allowed_roles: list[UserRole]):
    """
    Factory function to create role-based authorization dependency
    
    Usage:
        @app.get("/admin", dependencies=[Depends(require_role([UserRole.ADMIN]))])
        async def admin_endpoint():
            ...
    """
    async def role_checker(current_user: UserProxy = Depends(get_current_active_user)) -> UserProxy:
        # Ensure we're comparing enum values
        user_role = current_user.role
        if isinstance(user_role, str):
            try:
                user_role = UserRole(user_role.lower())
            except ValueError:
                # Handle case where role doesn't match enum
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Invalid user role: {user_role}"
                )
        
        # Check if user's role is in allowed roles
        if user_role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Not enough permissions. Required roles: {[r.value for r in allowed_roles]}, but user has role: {user_role.value if isinstance(user_role, UserRole) else user_role}"
            )
        return current_user
    
    return role_checker