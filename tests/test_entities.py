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