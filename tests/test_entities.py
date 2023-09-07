import os

import neo4j
import pytest

from bitcoingraph.entities import EntityGrouping, get_addresses_grouped_by_transaction


@pytest.fixture
def session():
    host = os.environ.get("NEO4J_HOST", "127.0.0.1")
    port = os.environ.get("NEO4J_PORT", "7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    psw = os.environ.get("NEO4J_PASSWORD", "neo4j")

    driver = neo4j.GraphDatabase.driver(
        f"bolt://{host}:{port}",
        auth=(user, psw),
        connection_timeout=3600)

    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        yield session

    driver.close()

def test_get_addresses_grouped_by_transaction(session):
    session.run("""
    MERGE (b1:Block {height: 1})-[:CONTAINS]->(t1:Transaction {txid: 1})
    MERGE (t1)<-[:INPUT]-(o11:Output)
    MERGE (t1)<-[:INPUT]-(o12:Output)
    MERGE (o11)-[:USES]->(a11:Address {address: "11"})
    MERGE (o12)-[:USES]->(a12:Address {address: "12"})
    MERGE (a11)-[:GENERATES]->(a11g:Address {address: "11_1g"})
    MERGE (a11)-[:GENERATES]->(a12g:Address {address: "11_2g"})
    MERGE (a12f:Address {address: "12father"})-[:GENERATES]->(a12)
    MERGE (b1)-[:CONTAINS]->(t2:Transaction {txid: 2})
    MERGE (t2)<-[:INPUT]-(o21:Output)
    MERGE (t2)<-[:INPUT]-(o22:Output)
    MERGE (o21)-[:USES]->(a21:Address {address: "21"})
    MERGE (o22)-[:USES]->(a22:Address {address: "22"})
    """)

    rows = get_addresses_grouped_by_transaction(session, 1, 1)

    assert len(rows) == 2
    assertions = 0

    for row in rows:
        if len(row["generated"]):
            assertions |= 1
            assert set(row["addresses"]) == {'11', '12'}
            assert set(row["generated"]) == {'11_1g', '11_2g', '12father'}
        else:
            assertions |= 2
            assert set(row["addresses"]) == {'21', '22'}
            assert set(row["generated"]) == set([])

    assert assertions == 3


class TestSaveEntities():
    def test_separate_no_entities(self, session):
        session.run("""
        MERGE (t1:Address {address: "1"})
        MERGE (t2:Address {address: "2"})
        """)

        grouping = EntityGrouping()
        grouping.entity_idx_to_addresses = {"1": ["1", "2"]}

        def run_save():
            grouping.save_entities(session, False)
            response = session.run("""
            MATCH (a1:Address)<-[:OWNER_OF]-(e:Entity)-[:OWNER_OF]->(a2:Address)
            WHERE a1.address = "1" AND a2.address = "2"
            RETURN a1,a2,e
            """)
            return response.data()

        result = run_save()
        assert len(result) == 1
        assert result[0]["e"]["entity_id"] == "1"

        # check that re-running the same query doesn't alter the result
        result = run_save()
        assert len(result) == 1
        assert result[0]["e"]["entity_id"] == "1"

    def test_separate_one_has_entity(self, session):
        session.run("""
        MERGE (t1:Address {address: "1"})<-[:OWNER_OF]-(e:Entity {entity_id: "123"})
        MERGE (t2:Address {address: "2"})
        """)

        grouping = EntityGrouping()
        grouping.entity_idx_to_addresses = {"1": ["1", "2"]}

        def run_save():
            grouping.save_entities(session, False)
            response = session.run("""
            MATCH (a1:Address)<-[:OWNER_OF]-(e:Entity)-[:OWNER_OF]->(a2:Address)
            WHERE a1.address = "1" AND a2.address = "2"
            RETURN a1,a2,e
            """)
            return response.data()

        data = run_save()
        assert len(data) == 1
        assert data[0]["e"]["entity_id"] == "123"

        # re-run to make sure the same query doesn't modify data
        data = run_save()
        assert len(data) == 1
        assert data[0]["e"]["entity_id"] == "123"

    def test_separate_both_have_entity(self, session):
        session.run("""
        MERGE (t1:Address {address: "1"})<-[:OWNER_OF]-(e1:Entity {entity_id: "123"})
        MERGE (t2:Address {address: "2"})<-[:OWNER_OF]-(e2:Entity {entity_id: "456"})
        """)

        grouping = EntityGrouping()
        grouping.entity_idx_to_addresses = {"1": ["1", "2"]}

        def run_save():
            grouping.save_entities(session, False)
            response = session.run("""
            MATCH (a1:Address)<-[:OWNER_OF]-(e:Entity)-[:OWNER_OF]->(a2:Address)
            WHERE a1.address = "1" AND a2.address = "2"
            RETURN a1,a2,e
            """)
            return response.data()

        data = run_save()
        assert len(data) == 1
        assert data[0]["e"]["entity_id"] == "123"

        data = session.run("""MATCH (e:Entity {entity_id: "456"}) RETURN e""").data()
        assert len(data) == 0

        # re-run
        data = run_save()
        assert len(data) == 1
        assert data[0]["e"]["entity_id"] == "123"

        data = session.run("""MATCH (e:Entity {entity_id: "456"}) RETURN e""").data()
        assert len(data) == 0

    def test_separate_three_have_entity(self, session):
        session.run("""
        MERGE (t1:Address {address: "1"})<-[:OWNER_OF]-(e1:Entity {entity_id: "123"})
        MERGE (t2:Address {address: "2"})<-[:OWNER_OF]-(e2:Entity {entity_id: "456"})
        MERGE (t3:Address {address: "3"})<-[:OWNER_OF]-(e3:Entity {entity_id: "789"})
        """)

        grouping = EntityGrouping()
        grouping.entity_idx_to_addresses = {"1": ["1", "2", "3"]}

        def run_save():
            grouping.save_entities(session, False)
            response = session.run("""
            MATCH (e:Entity)
            WITH e
            OPTIONAL MATCH (e)-[:OWNER_OF]->(a:Address)
            RETURN e, collect(a) as addresses
            """)
            return response.data()

        data = run_save()
        assert len(data) == 1
        assert data[0]["e"]["entity_id"] == "123"
        assert len(data[0]["addresses"])
        assert set((x["address"] for x in data[0]["addresses"])) == {"1", "2", "3"}

        # re-run
        data = run_save()
        assert len(data) == 1
        assert data[0]["e"]["entity_id"] == "123"
        assert len(data[0]["addresses"])
        assert set((x["address"] for x in data[0]["addresses"])) == {"1", "2", "3"}

    def test_merge_names_present(self, session):
        session.run("""
        MERGE (t1:Address {address: "1"})<-[:OWNER_OF]-(e1:Entity {entity_id: "123", name: "foo"})
        MERGE (t2:Address {address: "2"})<-[:OWNER_OF]-(e2:Entity {entity_id: "456", name: "bar"})
        MERGE (t3:Address {address: "3"})
        """)

        grouping = EntityGrouping()
        grouping.entity_idx_to_addresses = {"1": ["1", "2", "3"]}

        grouping.save_entities(session, False)
        response = session.run("""
        MATCH (e:Entity)
        WITH e
        OPTIONAL MATCH (e)-[:OWNER_OF]->(a:Address)
        RETURN e, collect(a) as addresses
        """)
        data = response.data()

        assert len(data) == 1
        assert data[0]["e"]["entity_id"] == "123"
        assert data[0]["e"]["name"] == "foo+bar"
        assert len(data[0]["addresses"])
        assert set((x["address"] for x in data[0]["addresses"])) == {"1", "2", "3"}

    def test_merge_names_none(self, session):
        session.run("""
                MERGE (t1:Address {address: "1"})<-[:OWNER_OF]-(e1:Entity {entity_id: "123"})
                MERGE (t2:Address {address: "2"})<-[:OWNER_OF]-(e2:Entity {entity_id: "456"})
                MERGE (t3:Address {address: "3"})
                """)

        grouping = EntityGrouping()
        grouping.entity_idx_to_addresses = {"1": ["1", "2", "3"]}

        grouping.save_entities(session, False)
        response = session.run("""
                MATCH (e:Entity)
                WITH e
                OPTIONAL MATCH (e)-[:OWNER_OF]->(a:Address)
                RETURN e, collect(a) as addresses
                """)
        data = response.data()

        assert len(data) == 1
        assert data[0]["e"]["entity_id"] == "123"
        assert data[0]["e"].get("name") is None
        assert len(data[0]["addresses"])
        assert set((x["address"] for x in data[0]["addresses"])) == {"1", "2", "3"}

    def test_merge_names_first_exists(self, session):
        session.run("""
                MERGE (t1:Address {address: "1"})<-[:OWNER_OF]-(e1:Entity {entity_id: "123", name: "foo"})
                MERGE (t2:Address {address: "2"})<-[:OWNER_OF]-(e2:Entity {entity_id: "456"})
                MERGE (t3:Address {address: "3"})
                """)

        grouping = EntityGrouping()
        grouping.entity_idx_to_addresses = {"1": ["1", "2", "3"]}

        grouping.save_entities(session, False)
        response = session.run("""
                MATCH (e:Entity)
                WITH e
                OPTIONAL MATCH (e)-[:OWNER_OF]->(a:Address)
                RETURN e, collect(a) as addresses
                """)
        data = response.data()

        assert len(data) == 1
        assert data[0]["e"]["entity_id"] == "123"
        assert data[0]["e"]["name"] == "foo"
        assert len(data[0]["addresses"])
        assert set((x["address"] for x in data[0]["addresses"])) == {"1", "2", "3"}

    def test_merge_names_second_exists(self, session):
        session.run("""
                MERGE (t1:Address {address: "1"})<-[:OWNER_OF]-(e1:Entity {entity_id: "123"})
                MERGE (t2:Address {address: "2"})<-[:OWNER_OF]-(e2:Entity {entity_id: "456", name: "foo"})
                MERGE (t3:Address {address: "3"})
                """)

        grouping = EntityGrouping()
        grouping.entity_idx_to_addresses = {"1": ["1", "2", "3"]}

        grouping.save_entities(session, False)
        response = session.run("""
                MATCH (e:Entity)
                WITH e
                OPTIONAL MATCH (e)-[:OWNER_OF]->(a:Address)
                RETURN e, collect(a) as addresses
                """)
        data = response.data()

        assert len(data) == 1
        assert data[0]["e"]["entity_id"] == "123"
        assert data[0]["e"]["name"] == "foo"
        assert len(data[0]["addresses"])
        assert set((x["address"] for x in data[0]["addresses"])) == {"1", "2", "3"}


class TestMergeEntities:
    def test_simple_case(self, session):
        session.run("""
                // Two entities
                MERGE (a1:Address {address: "1"})<-[:OWNER_OF]-(e1:Entity {entity_id: 1})
                MERGE (a2:Address {address: "2"})<-[:OWNER_OF]-(e2:Entity {entity_id: 2})
                MERGE (a1)-[:GENERATES]->(a2)
                """)

        session.run("""
        MATCH (e1:Entity)-[:OWNER_OF]->(a1:Address)-[:GENERATES]->(a2:Address)<-[:OWNER_OF]-(e2:Entity)
        WITH [e1,e2] as entities
        CALL apoc.refactor.mergeNodes(entities)
        YIELD node
        RETURN node
        """)

    def test_generator_has_entity(self, session):
        session.run("""
        MERGE (a1:Address {address: "1"})<-[:OWNER_OF]-(e1:Entity {entity_id: 1})
        MERGE (a1)-[:GENERATES]->(a2)
        """)

    def test_generated_has_entity(self, session):
        session.run("""
        MERGE (a1:Address {address: "1"})<-[:OWNER_OF]-(e1:Entity {entity_id: 1})
        MERGE (a2:Address {address: "2"})<-[:OWNER_OF]-(e2:Entity {entity_id: 2})
        MERGE (a1)-[:GENERATES]->(a2)
        """)

    def test_circular_dependence(self, session):
        merge_query = []
        for i in range(6):
            merge_query.append(f"MERGE (a{i+1}:Address {{address: '{i}'}})")

        merge_query = "\n".join(merge_query)
        session.run(merge_query + """
        MERGE (a1)<-[:OWNER_OF]-(e1:Entity {entity_id: 1})
        MERGE (a1)-[:GENERATES]->(a2)
        MERGE (a2)<-[:OWNER_OF]-(e2:Entity {entity_id: 2})
        MERGE (e2)-[:OWNER_OF]->(a3)
        MERGE (a3)-[:GENERATES]->(a4)
        MERGE (a4)<-[:OWNER_OF]-(e3:Entity {entity_id: 3})
        MERGE (e3)-[:OWNER_OF]->(a5)
        MERGE (a5)<-[:GENERATES]-(a6)
        MERGE (e1)-[:OWNER_OF]->(a6)
        """)