import neo4j
import pytest

from bitcoingraph.entities import EntityGrouping


@pytest.fixture
def session():
    driver = neo4j.GraphDatabase.driver(f"bolt://127.0.0.1:7687",
                                        auth=("neo4j", "neo4j"),
                                        connection_timeout=3600)

    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        yield session

    driver.close()


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
