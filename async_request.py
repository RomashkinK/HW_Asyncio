import asyncio
import datetime

import aiohttp
from more_itertools import chunked

import json

from models import Session, SwapiPeople, init_orm, engine

MAX_REQUESTS = 5


async def get_persons(client, person_id):
    response = await client.get(f'https://swapi.dev/api/people/{person_id}')
    if response.content_type == 'application/json':
        json_result = await response.json()
        return json_result
    else:
        return f"Unexpected content type: {response.content_type}"


async def main():
    await init_orm()
    client = aiohttp.ClientSession()
    for chunk in chunked(range(1, 101), MAX_REQUESTS):
        person_list = []
        for person_id in chunk:
            coro_person = get_persons(client, person_id)
            person_list.append(coro_person)
        result = await asyncio.gather(*person_list)
        asyncio.create_task(fill_db(result))
    tasks_set = asyncio.all_tasks()
    tasks_set.remove(asyncio.current_task())
    await asyncio.gather(*tasks_set)
    await client.close()
    await engine.dispose()


async def fill_db(list_of_jsons):
    for item in list_of_jsons:
        if 'name' in item:
            films_str = json.dumps(item['films'])
            starships_str = json.dumps(item['starships'])
            vehicles_str = json.dumps(item['vehicles'])
            species_str = json.dumps(item['species'])
            model = SwapiPeople(birth_year=item['birth_year'], 
                                eye_color=item['eye_color'],
                                films=films_str, 
                                gender=item['gender'], 
                                hair_color=item['hair_color'],
                                height=item['height'], 
                                homeworld=item['homeworld'], 
                                mass=item['mass'], 
                                name=item['name'],
                                skin_color=item['skin_color'], 
                                species=species_str, 
                                starships=starships_str,
                                vehicles=vehicles_str)
            async with Session() as session:
                session.add(model)
                await session.commit()

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)



