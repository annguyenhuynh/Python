from typing import NewType, Any, TypeVar
from dataclasses import dataclass
import random

RGB = NewType("RGB", tuple[int, int, int])

T = TypeVar("T") # What type goes in here is the type that comes out

def random_choice(items: list[T]) -> T:
    return random.choice(items)

@dataclass
class User:
    """
    @dataclass writes boring class code for you automatically.

    Specifically, it generates:

        __init__

        __repr__

        __eq__
    We can skip boilerplate steps like self.<var_name>
    """
    first_name: str
    last_name: str
    email: str
    age: int | None = None
    fav_color: RGB | None = None


def create_user(
    first_name: str,
    last_name: str,
    age: int | None = None,
    fav_color: tuple[int, int, int] | None = None,
) -> User:
    
    email = f"{first_name.lower()}_{last_name.lower()}@example.com"
    return User(
        first_name=first_name,
        last_name=last_name,
        email=email,
        age=age,
        fav_color=fav_color,
    )

user_1 = create_user("Kim", "Huynh", age=60, fav_color=(100,30,204))
user_2 = create_user("John", "Doe", fav_color=(103, 36, 204))

users = [user_1, user_2]

rando_users = random_choice(users)
print(rando_users)

emails = [user.email for user in users]
print(random_choice(emails))