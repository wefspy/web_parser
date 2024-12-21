import asyncio
from contextlib import asynccontextmanager
from typing import Annotated, Sequence

import httpx
from bs4 import BeautifulSoup
from fastapi import Depends, FastAPI
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Field, SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

sqlite_file_name = "database.db"
sqlite_url = f"sqlite+aiosqlite:///{sqlite_file_name}"

engine = create_async_engine(sqlite_url, echo=True)


async def create_db_and_tables():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def get_session() -> AsyncSession:
    async with AsyncSession(engine) as session:
        yield session

@asynccontextmanager
async def get_session_cm() -> AsyncSession:
    async with AsyncSession(engine) as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


class ProductEntity(SQLModel, table=True):
    id: int = Field(primary_key=True)
    name: str | None = Field(index=True)
    price: float | None = Field(default=None)


class Product(BaseModel):
    name: str
    price: float | None


BASE_URL = 'https://www.maxidom.ru/'
CATEGORY_URL = "https://www.maxidom.ru/catalog/sadovaya-tehnika/"


async def get_products(url: str) -> list[Product]:
    products: list[Product] = []
    async with httpx.AsyncClient() as client:
        while url:
            response = await client.get(url)
            soup: BeautifulSoup = BeautifulSoup(response.content, "lxml")

            articles = soup.find_all('article', class_='l-product')

            for product in articles:
                name = product.find('span', itemprop='name').get_text(strip=True)
                price = product.find('span', itemprop='price').get_text(strip=True)
                products.append(Product(name=name, price=price))

            next_page = soup.select_one('#navigation_2_next_page[href]')
            if next_page:
                url = BASE_URL + next_page['href']
            else:
                url = None
    return products


app = FastAPI()


@app.on_event("startup")
async def on_startup():
    await create_db_and_tables()
    asyncio.create_task(update_all_products_everyday())


async def update_all_products_everyday():
    while True:
        async with get_session_cm() as session:
            await update_product_category(session)
        await asyncio.sleep(60 * 60 * 24)


@app.post("/products/")
async def create_product(product: Product, session: SessionDep) -> ProductEntity:
    db_product = ProductEntity(name=product.name, price=product.price)
    session.add(db_product)
    await session.commit()
    await session.refresh(db_product)
    return db_product


@app.get("/products/{offset}/{limit}")
async def read_products_limit(
        session: SessionDep,
        offset: int,
        limit: int
) -> Sequence[ProductEntity]:
    result = await session.exec(select(ProductEntity).offset(offset).limit(limit))
    products = result.all()
    return products


@app.get("/products/")
async def read_products_limit(session: SessionDep) -> Sequence[ProductEntity]:
    result = await session.exec(select(ProductEntity))
    products = result.all()
    return products


@app.get("/products/{product_id}")
async def read_product(product_id: int, session: SessionDep) -> dict[str, str] | ProductEntity:
    product = await session.get(ProductEntity, product_id)
    if not product:
        return {"message": "Product not found"}
    return product


@app.delete("/products/{product_id}")
async def delete_product(product_id: int, session: SessionDep):
    product = await session.get(ProductEntity, product_id)
    if not product:
        return {"message": "Product not found"}
    await session.delete(product)
    await session.commit()
    return {"ok": True}


@app.put("/products/category/url")
async def set_url(session: SessionDep, url: str):
    global CATEGORY_URL
    if not url.startswith(BASE_URL):
        return {"message": "Incorrect URL"}

    CATEGORY_URL = url

    return {"ok": True, "updated_url": CATEGORY_URL}


@app.put("/products/category")
async def update_product_category(session: SessionDep):
    products = await get_products(CATEGORY_URL)

    await update_products_db(session, products)

    return {"ok": True, "updated_products": len(products)}


async def update_products_db(session: SessionDep, products: list[Product]):
    for product in products:
        db_product = await session.exec(select(ProductEntity).where(ProductEntity.name == product.name))
        db_product = db_product.first()

        if db_product:
            db_product.price = product.price
        else:
            db_product = ProductEntity(name=product.name, price=product.price)
            session.add(db_product)

    await session.commit()


@app.put("/products/{product_id}")
async def update_product(
        session: SessionDep,
        product_id: int,
        product_data: Product,
):
    db_product = await session.get(ProductEntity, product_id)
    if not db_product:
        return {"message": "Product not found"}

    db_product.name = product_data.name
    db_product.price = product_data.price

    session.add(db_product)
    await session.commit()
    await session.refresh(db_product)

    return db_product
