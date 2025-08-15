import pyodbc
import os
import requests

def akv_query_canvas_graphql(query, variable):
    """
    Send a GraphQL query to Canvas and return the response.

    :param query: GraphQL query
    :type query: str
    :param variable: GraphQL variable
    :type variable: dict
    :return: JSON response
    :rtype: dict
    :raises Exception: if the request fails
    """
    hode = {
        'Authorization': f'Bearer {os.environ["tokenCanvas"]}',
    }
    print(hode)
    GraphQLurl = "https://hvl.instructure.com/api/graphql/"
    svar = requests.post(
        GraphQLurl, 
        json = {
            'query': query,
            'variables': variable
        },
        headers=hode)
    # print(svar.content)
    if 200 <= svar.status_code < 300:
        return svar.json()
    else:
        raise Exception(f"Feil i spørjing med kode {svar.status_code}. {query}")



conn_str = os.environ['Connection_SQL']
with pyodbc.connect(conn_str) as connection:
    cursor = connection.cursor()
    try:
        query = """
        SELECT * FROM stg.Canvas_Courses
        """
        cursor.execute(query)
        row = cursor.fetchall()
        aktuelle_emne = []  # eller .fetchall
        for emne in row:
            if emne[4] == 332:
                aktuelle_emne.append(emne[0])
        print(aktuelle_emne)
    except pyodbc.Error as exc:
        print("Feil")

queryCanvas = """
query MyQuery($courseId: ID!) {
    course(id: $courseId) {
        enrollmentsConnection {
            nodes {
                user {
                    _id
                    sisId
                    createdAt
                    updatedAt
                    name
                }
                type
                state
                _id
                totalActivityTime
                lastActivityAt
            }
        }
    }
}
"""

for emne in aktuelle_emne[0:1]:
    print(emne)
    variables = {"courseId": emne}
    
    resultat = akv_query_canvas_graphql(queryCanvas, variables)
    enrollments = resultat['data']['course']['enrollmentsConnection']['nodes']
    print(enrollments)


