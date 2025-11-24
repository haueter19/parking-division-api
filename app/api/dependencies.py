from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.models.database import User, UserRole
from app.utils.auth import decode_access_token

# OAuth2 scheme for token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/auth/login")


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> User:
    """
    Dependency to get the current authenticated user from JWT token
    
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
    
    user = db.query(User).filter(User.username == username).first()
    if user is None:
        raise credentials_exception
    
    return user


async def get_current_active_user(
    current_user: User = Depends(get_current_user)
) -> User:
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
    async def role_checker(current_user: User = Depends(get_current_active_user)) -> User:
        # Ensure we're comparing enum values
        user_role = current_user.role
        if isinstance(user_role, str):
            user_role = UserRole(user_role)
        
        # Check if user's role is in allowed roles
        if user_role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Not enough permissions. Required roles: {[r.value for r in allowed_roles]}, but user has role: {user_role.value if isinstance(user_role, UserRole) else user_role}"
            )
        return current_user
    
    return role_checker