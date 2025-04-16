from pydantic import BaseModel


class UserCreate(BaseModel):
    name: str
    email: str
    city: str


class UserResponse(UserCreate):
    id: int

    model_config = {
        "from_attributes": True
    }


class PostCreate(BaseModel):
    title: str
    content: str
    author_id: int


class PostResponse(PostCreate):
    id: int

    model_config = {
        "from_attributes": True
    }


class CommentCreate(BaseModel):
    post_id: int
    text: str
    commenter_name: str


class CommentResponse(CommentCreate):
    id: int

    model_config = {
        "from_attributes": True
    }


class ProductCreate(BaseModel):
    name: str
    price: int
    description: str


class ProductResponse(ProductCreate):
    id: int

    model_config = {
        "from_attributes": True
    }
