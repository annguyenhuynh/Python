from typing import NewType, TypedDict
from dataclasses import dataclass

RGB = NewType("RGB", tuple[int, int, int])
HSL = NewType("HSL", tuple[int, int, int])

@dataclass
class User:
    first_name: str
    last_name: str
    age: int | None = None
    fav_color: RGB | None = None

# type User = dict[str, str | int | RGB | None]


def create_user(
        first_name:str, 
        last_name:str, 
        age:int | None = None,
        fav_color: RGB | None = None) -> User:
    
    email = f"{first_name.lower()}_{last_name.lower()}@example.com"


    return User(
        first_name=first_name,
        last_name= last_name,
        email= email,
        age=age,
        fav_color=fav_color
    )
    
    
 
    # return {
    #     "first_name": first_name,
    #     "last_name": last_name,
    #     "email": email,
    #     "age": str_age  # This does not return error because values can be any type defined in type User --> better way is to use TypeDict
    # }
  