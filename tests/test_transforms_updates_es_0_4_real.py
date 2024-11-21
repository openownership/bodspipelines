import json
import os
import pytest

from bodspipelines.infrastructure.utils import current_date_iso

from .config import set_environment_variables
from .run_pipeline import run_transform_pipeline

# Setup environment variables
set_environment_variables()

@pytest.fixture(scope="module")
def wait_for_es(module_scoped_container_getter):
    service = module_scoped_container_getter.get("bods_ingester_gleif_es").network_info[0]
    os.environ['ELASTICSEARCH_HOST'] = service.hostname
    os.environ['ELASTICSEARCH_PORT'] = service.host_port
    return service

@pytest.fixture
def lei_json_data_file_gleif():
    """GLEIF LEI Record update"""
    with open("tests/fixtures/lei-updates-replaces-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def relationship_update_data_file_gleif():
    """GLEIF LEI and RR Records update"""
    with open("tests/fixtures/relationship-update-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def relationship_update_lei_data_file_gleif():
    """GLEIF LEI Record update with existing relationships"""
    with open("tests/fixtures/relationship-update-lei-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def reporting_exceptions_data_file_gleif():
    """GLEIF LEI and RR Records update"""
    with open("tests/fixtures/reporting-exceptions-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_retired_data_file_gleif():
    """GLEIF LEI retired update"""
    with open("tests/fixtures/lei-retired-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_updates_retired_data_file_gleif():
    """GLEIF LEI retired update"""
    with open("tests/fixtures/lei-updates-retired-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def repex_updates_deletion_data_file_gleif():
    """GLEIF LEI retired update"""
    with open("tests/fixtures/repex-updates-deletion-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def relationship_update_replace_repex_data_file_gleif():
    """GLEIF LEI retired update"""
    with open("tests/fixtures/relationship-update-replace-repex-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_repex_replaced_and_deleted_data():
    """GLEIF LEI retired update"""
    with open("tests/fixtures/lei-repex-replaced-and-deleted.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_repex_replaced_and_deleted_data2():
    """GLEIF LEI retired update"""
    with open("tests/fixtures/lei-repex-replaced-and-deleted2.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def multiple_updates_data():
    """GLEIF LEI and RR with multiple updates"""
    with open("tests/fixtures/multiple-updates.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def multiple_updates_data2():
    """GLEIF LEI, RR and Repex with multiple updates"""
    with open("tests/fixtures/multiple-updates2.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def multiple_updates_data3():
    """GLEIF LEI, RR and Repex with multiple updates"""
    with open("tests/fixtures/multiple-updates3.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def multiple_updates_data4():
    """GLEIF LEI, RR and Repex with multiple updates"""
    with open("tests/fixtures/multiple-updates4.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def multiple_updates_data5():
    """GLEIF LEI, RR and Repex with multiple updates"""
    with open("tests/fixtures/multiple-updates5.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def multiple_updates_data6():
    """GLEIF LEI, RR and Repex with multiple updates"""
    with open("tests/fixtures/multiple-updates6.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def multiple_updates_data7():
    """GLEIF LEI, RR and Repex with multiple updates"""
    with open("tests/fixtures/multiple-updates7.json", "r") as read_file:
        return json.load(read_file)


@pytest.mark.asyncio
@pytest.mark.order(1)
async def test_lei_replaces(wait_for_es, lei_json_data_file_gleif):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(lei_json_data_file_gleif)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 2

    assert output_stream[0]["statementId"] == "fb6c008f-dca6-58d1-4e07-d194333a19ac"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-549300JV7BZB002LHI61"
    assert output_stream[0]["statementDate"] == "2023-12-29"
    assert output_stream[0]["recordId"] == "XI-LEI-549300JV7BZB002LHI61"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "2c626487-af92-173b-7dd9-2d6216ef5590"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-549300JV7BZB002LHI61"
    assert output_stream[1]["statementDate"] == "2024-01-03"
    assert output_stream[1]["recordId"] == "XI-LEI-549300JV7BZB002LHI61"
    assert output_stream[1]["recordStatus"] == "updated"
    assert output_stream[1]["recordType"] == "entity"

@pytest.mark.asyncio
@pytest.mark.order(2)
async def test_relationship_replaces(wait_for_es, relationship_update_data_file_gleif):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(relationship_update_data_file_gleif)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 6

    assert output_stream[0]["statementId"] == "784c8a7a-1f61-ac31-f4f0-118e92e06e79"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-0292003540H0S4VA7A50"
    assert output_stream[0]["statementDate"] == "2023-02-22"
    assert output_stream[0]["recordId"] == "XI-LEI-0292003540H0S4VA7A50"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "76237ac9-2e52-4101-ef80-6b34424b06d7"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-0292003540H0S4VA7A50"
    assert output_stream[1]["statementDate"] == "2023-02-22"
    assert output_stream[1]["recordId"] == "XI-LEI-RR-D-0292003540H0S4VA7A50-0292002717G4T0CH6E65"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "881c7f80-20fb-ad95-5de9-d643c105b720"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-0292003540H0S4VA7A50"
    assert output_stream[2]["statementDate"] == "2023-02-22"
    assert output_stream[2]["recordId"] == "XI-LEI-RR-U-0292003540H0S4VA7A50-0292002717G4T0CH6E65"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "e080d839-e117-e745-89dd-bcef2eb4ac46"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-0292003540H0S4VA7A50"
    assert output_stream[3]["statementDate"] == "2024-01-30"
    assert output_stream[3]["recordId"] == "XI-LEI-0292003540H0S4VA7A50"
    assert output_stream[3]["recordStatus"] == "updated"
    assert output_stream[3]["recordType"] == "entity"

    assert output_stream[4]["statementId"] == "f3d6a24b-c294-c942-e9db-d8c39aed98fb"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-0292003540H0S4VA7A50"
    assert output_stream[4]["statementDate"] == "2024-01-30"
    assert output_stream[4]["recordId"] == "XI-LEI-RR-D-0292003540H0S4VA7A50-0292002717G4T0CH6E65"
    assert output_stream[4]["recordStatus"] == "updated"
    assert output_stream[4]["recordType"] == "relationship"

    assert output_stream[5]["statementId"] == "c4adae55-a7c5-5458-0204-dfb70f82d6f1"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-0292003540H0S4VA7A50"
    assert output_stream[5]["statementDate"] == "2024-01-30"
    assert output_stream[5]["recordId"] == "XI-LEI-RR-U-0292003540H0S4VA7A50-0292002717G4T0CH6E65"
    assert output_stream[5]["recordStatus"] == "updated"
    assert output_stream[5]["recordType"] == "relationship"

@pytest.mark.asyncio
@pytest.mark.order(3)
async def test_reporting_exceptions(wait_for_es, reporting_exceptions_data_file_gleif):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(reporting_exceptions_data_file_gleif)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 3

    assert output_stream[0]["statementId"] == "a3988cc9-376f-24b3-e3e4-3a25d2661dac"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-959800W87BKGBUDPF915"
    assert output_stream[0]["statementDate"] == "2022-01-25"
    assert output_stream[0]["recordId"] == "XI-LEI-959800W87BKGBUDPF915"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "7efdeb0a-da89-e043-ca75-d5ab7dc56465"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-959800W87BKGBUDPF915"
    assert output_stream[1]["statementDate"] == "2024-01-01"
    assert output_stream[1]["recordId"] == "XI-LEI-RE-D-959800W87BKGBUDPF915"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "e8efadd2-77d7-59e3-3451-97c9d25b81db"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-959800W87BKGBUDPF915"
    assert output_stream[2]["statementDate"] == "2024-01-01"
    assert output_stream[2]["recordId"] == "XI-LEI-RE-U-959800W87BKGBUDPF915"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

@pytest.mark.asyncio
@pytest.mark.order(4)
async def test_retired_lei(wait_for_es, lei_retired_data_file_gleif):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(lei_retired_data_file_gleif)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 2

    assert output_stream[0]["statementId"] == "f8a78aed-7ca3-15b6-ecb6-e491f666d59b"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-213800KU9T4Z3TEL8133"
    assert output_stream[0]["statementDate"] == "2023-03-21"
    assert output_stream[0]["recordId"] == "XI-LEI-213800KU9T4Z3TEL8133"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "f0a54313-d89d-7111-32d5-00e70d90d8a3"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-213800KU9T4Z3TEL8133"
    assert output_stream[1]["statementDate"] == "2024-01-07"
    assert output_stream[1]["recordId"] == "XI-LEI-213800KU9T4Z3TEL8133"
    assert output_stream[1]["recordStatus"] == "closed"
    assert output_stream[1]["recordType"] == "entity"

@pytest.mark.asyncio
@pytest.mark.order(5)
async def test_retired_lei_updates(wait_for_es, lei_updates_retired_data_file_gleif):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(lei_updates_retired_data_file_gleif)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 4

    assert output_stream[0]["statementId"] == "58dc4441-ad1f-7a96-081a-bfc02c6d6971"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-W6WH36E2JKQ9TZEL6L82"
    assert output_stream[0]["statementDate"] == "2023-11-15"
    assert output_stream[0]["recordId"] == "XI-LEI-W6WH36E2JKQ9TZEL6L82"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "dd6e45bd-b6f5-9f26-28ff-6b1dd5789186"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-W6WH36E2JKQ9TZEL6L82"
    assert output_stream[1]["statementDate"] == "2024-01-01"
    assert output_stream[1]["recordId"] == "XI-LEI-RE-U-W6WH36E2JKQ9TZEL6L82"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "cb512374-ea4d-e53e-0f98-42f718257431"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-W6WH36E2JKQ9TZEL6L82"
    assert output_stream[2]["statementDate"] == "2024-01-01"
    assert output_stream[2]["recordId"] == "XI-LEI-RE-D-W6WH36E2JKQ9TZEL6L82"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "9ce5dad7-e0e3-cef9-a49a-0cd6bb5c7bb9"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-W6WH36E2JKQ9TZEL6L82"
    assert output_stream[3]["statementDate"] == "2024-01-12"
    assert output_stream[3]["recordId"] == "XI-LEI-W6WH36E2JKQ9TZEL6L82"
    assert output_stream[3]["recordStatus"] == "closed"
    assert output_stream[3]["recordType"] == "entity"

@pytest.mark.asyncio
@pytest.mark.order(6)
async def test_repex_deletion_update(wait_for_es, repex_updates_deletion_data_file_gleif):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(repex_updates_deletion_data_file_gleif)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 8

    assert output_stream[0]["statementId"] == "5b000f15-f472-92b7-6155-6e898411210c"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-549300IULECH8AURP345"
    assert output_stream[0]["statementDate"] == "2023-08-10"
    assert output_stream[0]["recordId"] == "XI-LEI-549300IULECH8AURP345"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "e796e4f0-bbd0-14a5-b88f-67f437ccdc2d"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-549300IULECH8AURP345"
    assert output_stream[1]["statementDate"] == "2023-08-10"
    assert output_stream[1]["recordId"] == "XI-LEI-RR-D-549300IULECH8AURP345-549300LASJT0ZLZ3WT59"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "c23ef197-eb6e-855e-75b1-5f20a0521582"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-549300EPVMOSB2AUNW89"
    assert output_stream[2]["statementDate"] == "2023-09-04"
    assert output_stream[2]["recordId"] == "XI-LEI-RR-D-549300EPVMOSB2AUNW89-549300IULECH8AURP345"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "e5b1d829-7c8e-4a21-e067-c7dece7b6fb1"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-549300IULECH8AURP345"
    assert output_stream[3]["statementDate"] == "2024-01-01"
    assert output_stream[3]["recordId"] == "XI-LEI-RE-U-549300IULECH8AURP345"
    assert output_stream[3]["recordStatus"] == "new"
    assert output_stream[3]["recordType"] == "relationship"

    assert output_stream[4]["statementId"] == "21d0506e-2435-3242-af84-ba4163f31e1f"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-549300IULECH8AURP345"
    assert output_stream[4]["statementDate"] == "2024-01-31"
    assert output_stream[4]["recordId"] == "XI-LEI-549300IULECH8AURP345"
    assert output_stream[4]["recordStatus"] == "updated"
    assert output_stream[4]["recordType"] == "entity"

    assert output_stream[5]["statementId"] == "8f80f457-64fa-015b-e077-0392d420a4f9"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-549300IULECH8AURP345"
    assert output_stream[5]["statementDate"] == "2024-01-31"
    assert output_stream[5]["recordId"] == "XI-LEI-RR-D-549300IULECH8AURP345-549300LASJT0ZLZ3WT59"
    assert output_stream[5]["recordStatus"] == "updated"
    assert output_stream[5]["recordType"] == "relationship"

    assert output_stream[6]["statementId"] == "6c7f70b1-9bc6-f76d-c071-d801ce2bbf27"
    assert output_stream[6]["declarationSubject"] == "XI-LEI-549300IULECH8AURP345"
    assert output_stream[6]["statementDate"] == "2024-01-31"
    assert output_stream[6]["recordId"] == "XI-LEI-RR-U-549300IULECH8AURP345-549300LASJT0ZLZ3WT59"
    assert output_stream[6]["recordStatus"] == "new"
    assert output_stream[6]["recordType"] == "relationship"

    assert output_stream[7]["statementId"] == "7960094c-355b-76e4-d0d1-cf51d3d158fe"
    assert output_stream[7]["declarationSubject"] == "XI-LEI-549300IULECH8AURP345"
    assert output_stream[7]["statementDate"] == "2024-02-01"
    assert output_stream[7]["recordId"] == "XI-LEI-RE-U-549300IULECH8AURP345"
    assert output_stream[7]["recordStatus"] == "closed"
    assert output_stream[7]["recordType"] == "relationship"

@pytest.mark.asyncio
@pytest.mark.order(7)
async def test_relationship_replace_repex_update(wait_for_es, relationship_update_replace_repex_data_file_gleif):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(relationship_update_replace_repex_data_file_gleif)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 4

    assert output_stream[0]["statementId"] == "26b1e1ad-d255-996b-2f6f-c3dde996f891"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-5493006SJVWFSGOKXU68"
    assert output_stream[0]["statementDate"] == "2023-08-04"
    assert output_stream[0]["recordId"] == "XI-LEI-5493006SJVWFSGOKXU68"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "ef94dc54-189b-511f-e956-8ed70e3cb5e1"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-5493006SJVWFSGOKXU68"
    assert output_stream[1]["statementDate"] == "2024-01-01"
    assert output_stream[1]["recordId"] == "XI-LEI-RE-D-5493006SJVWFSGOKXU68"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "640e51cb-768a-60a1-6db2-90381b322eb2"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-5493006SJVWFSGOKXU68"
    assert output_stream[2]["statementDate"] == "2023-12-21"
    assert output_stream[2]["recordId"] == "XI-LEI-RR-D-5493006SJVWFSGOKXU68-DYTQ8KRTKO7Y2BVU5K74"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "538b72c4-7862-fef3-9d6c-3996a68ab072"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-5493006SJVWFSGOKXU68"
    assert output_stream[3]["statementDate"] == current_date_iso()
    assert output_stream[3]["recordId"] == "XI-LEI-RE-D-5493006SJVWFSGOKXU68"
    assert output_stream[3]["recordStatus"] == "closed"
    assert output_stream[3]["recordType"] == "relationship"

@pytest.mark.asyncio
@pytest.mark.order(8)
async def test_lei_repex_replaced_and_deleted_update(wait_for_es, lei_repex_replaced_and_deleted_data):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(lei_repex_replaced_and_deleted_data)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 7

    assert output_stream[0]["statementId"] == "a09f5c4a-713b-406e-f305-394ccd157124"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-549300411SG58OR0YJ50"
    assert output_stream[0]["statementDate"] == "2023-11-22"
    assert output_stream[0]["recordId"] == "XI-LEI-549300411SG58OR0YJ50"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "267a0057-9bbe-16b8-c5ce-04fae9f0cd6d"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-549300411SG58OR0YJ50"
    assert output_stream[1]["statementDate"] == "2024-01-01"
    assert output_stream[1]["recordId"] == "XI-LEI-RE-D-549300411SG58OR0YJ50"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "9f5b445c-f9cd-883d-a658-6cb8dabdbdd0"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-549300411SG58OR0YJ50"
    assert output_stream[2]["statementDate"] == "2024-01-01"
    assert output_stream[2]["recordId"] == "XI-LEI-RE-U-549300411SG58OR0YJ50"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "ced72f6b-ba94-6791-4a83-0b3e1d1fc562"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-549300411SG58OR0YJ50"
    assert output_stream[3]["statementDate"] == "2024-01-12"
    assert output_stream[3]["recordId"] == "XI-LEI-RR-D-549300411SG58OR0YJ50-549300MW265RRCSDUE83"
    assert output_stream[3]["recordStatus"] == "new"
    assert output_stream[3]["recordType"] == "relationship"

    assert output_stream[4]["statementId"] == "35beac0c-0db3-d405-38bf-62cfa5bcf7c1"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-549300411SG58OR0YJ50"
    assert output_stream[4]["statementDate"] == "2024-01-12"
    assert output_stream[4]["recordId"] == "XI-LEI-RR-U-549300411SG58OR0YJ50-8I5DZWZKVSZI1NUHU748"
    assert output_stream[4]["recordStatus"] == "new"
    assert output_stream[4]["recordType"] == "relationship"

    assert output_stream[5]["statementId"] == "689ef45d-3897-ae5e-c093-9cca13521806"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-549300411SG58OR0YJ50"
    assert output_stream[5]["statementDate"] == "2024-02-01"
    assert output_stream[5]["recordId"] == "XI-LEI-RE-U-549300411SG58OR0YJ50"
    assert output_stream[5]["recordStatus"] == "closed"
    assert output_stream[5]["recordType"] == "relationship"

    assert output_stream[6]["statementId"] == "152fc7cc-e9b2-1c0d-3619-6665447098a3"
    assert output_stream[6]["declarationSubject"] == "XI-LEI-549300411SG58OR0YJ50"
    assert output_stream[6]["statementDate"] == "2024-02-01"
    assert output_stream[6]["recordId"] == "XI-LEI-RE-D-549300411SG58OR0YJ50"
    assert output_stream[6]["recordStatus"] == "closed"
    assert output_stream[6]["recordType"] == "relationship"

@pytest.mark.asyncio
@pytest.mark.order(9)
async def test_lei_repex_replaced_and_deleted2_update(wait_for_es, lei_repex_replaced_and_deleted_data2):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(lei_repex_replaced_and_deleted_data2)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 8

    assert output_stream[0]["statementId"] == "f66686d8-687f-826a-b124-6a761bef7ca3"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-894500Z71P8G4KVJWT89"
    assert output_stream[0]["statementDate"] == "2023-02-01"
    assert output_stream[0]["recordId"] == "XI-LEI-894500Z71P8G4KVJWT89"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "30619170-8b90-15e5-d789-946c763dd2fc"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-894500Z71P8G4KVJWT89"
    assert output_stream[1]["statementDate"] == "2024-01-01"
    assert output_stream[1]["recordId"] == "XI-LEI-RE-U-894500Z71P8G4KVJWT89"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "29369519-bf95-bc3e-2208-295dee946d55"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-894500Z71P8G4KVJWT89"
    assert output_stream[2]["statementDate"] == "2024-01-01"
    assert output_stream[2]["recordId"] == "XI-LEI-RE-D-894500Z71P8G4KVJWT89"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "f1f5f3c3-bef9-8fd6-1397-2675f325ec70"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-894500Z71P8G4KVJWT89"
    assert output_stream[3]["statementDate"] == "2024-01-26"
    assert output_stream[3]["recordId"] == "XI-LEI-RR-D-894500Z71P8G4KVJWT89-335800GIAQ18FYFOL151"
    assert output_stream[3]["recordStatus"] == "new"
    assert output_stream[3]["recordType"] == "relationship"

    assert output_stream[4]["statementId"] == "33e2f872-a88c-9661-87a8-397a3c56e2d0"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-894500Z71P8G4KVJWT89"
    assert output_stream[4]["statementDate"] == "2024-01-26"
    assert output_stream[4]["recordId"] == "XI-LEI-RR-U-894500Z71P8G4KVJWT89-5493003UOETFYRONLG31"
    assert output_stream[4]["recordStatus"] == "new"
    assert output_stream[4]["recordType"] == "relationship"

    assert output_stream[5]["statementId"] == "f515cb70-ba51-82ce-160b-c0ee1856c054"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-894500Z71P8G4KVJWT89"
    assert output_stream[5]["statementDate"] == "2024-01-26"
    assert output_stream[5]["recordId"] == "XI-LEI-894500Z71P8G4KVJWT89"
    assert output_stream[5]["recordStatus"] == "updated"
    assert output_stream[5]["recordType"] == "entity"

    assert output_stream[6]["statementId"] == "27e539c7-3c91-e11a-d189-bec3d7a6889b"
    assert output_stream[6]["declarationSubject"] == "XI-LEI-894500Z71P8G4KVJWT89"
    assert output_stream[6]["statementDate"] == "2024-02-01"
    assert output_stream[6]["recordId"] == "XI-LEI-RE-D-894500Z71P8G4KVJWT89"
    assert output_stream[6]["recordStatus"] == "closed"
    assert output_stream[6]["recordType"] == "relationship"

    assert output_stream[7]["statementId"] == "28bf9fb7-f040-6fcd-8198-5f4bb7012429"
    assert output_stream[7]["declarationSubject"] == "XI-LEI-894500Z71P8G4KVJWT89"
    assert output_stream[7]["statementDate"] == "2024-02-01"
    assert output_stream[7]["recordId"] == "XI-LEI-RE-U-894500Z71P8G4KVJWT89"
    assert output_stream[7]["recordStatus"] == "closed"
    assert output_stream[7]["recordType"] == "relationship"

@pytest.mark.asyncio
@pytest.mark.order(10)
async def test_multiple_updates_update(wait_for_es, multiple_updates_data):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(multiple_updates_data)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 9

    assert output_stream[0]["statementId"] == "953e46cb-b883-fc60-a136-a6e6ecbab6cd"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[0]["statementDate"] == "2023-05-04"
    assert output_stream[0]["recordId"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "491c0abe-5668-311c-1f78-2a8ca67f88c5"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[1]["statementDate"] == "2023-05-04"
    assert output_stream[1]["recordId"] == "XI-LEI-RR-D-2138004DSLJ1J9JYXU75-5493003BZYYYCDIO0R13"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "bfd4044f-8fa6-df30-ac91-90ec51263f0a"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[2]["statementDate"] == "2023-05-04"
    assert output_stream[2]["recordId"] == "XI-LEI-RR-U-2138004DSLJ1J9JYXU75-5493003BZYYYCDIO0R13"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "801e490e-7264-adc3-95d2-47a12993a33e"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[3]["statementDate"] == "2024-07-03"
    assert output_stream[3]["recordId"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[3]["recordStatus"] == "updated"
    assert output_stream[3]["recordType"] == "entity"

    assert output_stream[4]["statementId"] == "ab92f19a-5b57-ac7c-bcf4-e04401e2e1cb"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[4]["statementDate"] == "2024-07-03"
    assert output_stream[4]["recordId"] == "XI-LEI-RR-D-2138004DSLJ1J9JYXU75-5493003BZYYYCDIO0R13"
    assert output_stream[4]["recordStatus"] == "closed"
    assert output_stream[4]["recordType"] == "relationship"

    assert output_stream[5]["statementId"] == "e325f4d1-0393-39e1-cf5e-5fa383ca4d69"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[5]["statementDate"] == "2024-07-03"
    assert output_stream[5]["recordId"] == "XI-LEI-RR-U-2138004DSLJ1J9JYXU75-5493003BZYYYCDIO0R13"
    assert output_stream[5]["recordStatus"] == "closed"
    assert output_stream[5]["recordType"] == "relationship"

    assert output_stream[6]["statementId"] == "2cd21a0c-1d1f-5bbf-a030-e35706af8226"
    assert output_stream[6]["declarationSubject"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[6]["statementDate"] == "2024-07-09"
    assert output_stream[6]["recordId"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[6]["recordStatus"] == "updated"
    assert output_stream[6]["recordType"] == "entity"

    assert output_stream[7]["statementId"] == "dc7e0e07-e3bf-d298-710b-26b38e7e21b2"
    assert output_stream[7]["declarationSubject"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[7]["statementDate"] == "2024-07-09"
    assert output_stream[7]["recordId"] == "XI-LEI-RR-D-2138004DSLJ1J9JYXU75-5493003BZYYYCDIO0R13_2"
    assert output_stream[7]["recordStatus"] == "new"
    assert output_stream[7]["recordType"] == "relationship"

    assert output_stream[8]["statementId"] == "2b61ce08-f0e3-6d7f-7364-57f7f93a31f9"
    assert output_stream[8]["declarationSubject"] == "XI-LEI-2138004DSLJ1J9JYXU75"
    assert output_stream[8]["statementDate"] == "2024-07-09"
    assert output_stream[8]["recordId"] == "XI-LEI-RR-U-2138004DSLJ1J9JYXU75-5493003BZYYYCDIO0R13_2"
    assert output_stream[8]["recordStatus"] == "new"
    assert output_stream[8]["recordType"] == "relationship"

@pytest.mark.asyncio
@pytest.mark.order(11)
async def test_multiple_updates2_update(wait_for_es, multiple_updates_data2):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(multiple_updates_data2)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 12

    assert output_stream[0]["statementId"] == "ea71e379-f07d-8453-959a-f7d3a533054c"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[0]["statementDate"] == "2024-08-05"
    assert output_stream[0]["recordId"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "0e7d84a6-dc6c-d6af-8e11-a059cf3123aa"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[1]["statementDate"] == "2024-08-07"
    assert output_stream[1]["recordId"] == "XI-LEI-RE-D-5493004MMZ3PHA6QX343"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "d284b15d-1bbf-ad80-3df8-9831b641cd46"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[2]["statementDate"] == "2024-08-07"
    assert output_stream[2]["recordId"] == "XI-LEI-RE-U-5493004MMZ3PHA6QX343"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "fe081762-d0b1-687c-10e5-eb2b5a3ecc2f"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[3]["statementDate"] == "2023-09-19"
    assert output_stream[3]["recordId"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[3]["recordStatus"] == "updated"
    assert output_stream[3]["recordType"] == "entity"

    assert output_stream[4]["statementId"] == "fc8908ca-165a-8811-9deb-5e6e1d04c725"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-549300GMLRTJIOPMOJ75"
    assert output_stream[4]["statementDate"] == "2023-09-19"
    assert output_stream[4]["recordId"] == "XI-LEI-RR-D-549300GMLRTJIOPMOJ75-5493004MMZ3PHA6QX343"
    assert output_stream[4]["recordStatus"] == "new"
    assert output_stream[4]["recordType"] == "relationship"

    assert output_stream[5]["statementId"] == "20780a0b-3e54-0e8e-233c-3219817bf20c"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[5]["statementDate"] == "2023-09-19"
    assert output_stream[5]["recordId"] == "XI-LEI-RR-D-5493004MMZ3PHA6QX343-549300HMM4D75VUY8J80"
    assert output_stream[5]["recordStatus"] == "new"
    assert output_stream[5]["recordType"] == "relationship"

    assert output_stream[6]["statementId"] == "d01ad6ca-fba5-0801-4c20-0de521cc69c1"
    assert output_stream[6]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[6]["statementDate"] == "2023-09-19"
    assert output_stream[6]["recordId"] == "XI-LEI-RR-U-5493004MMZ3PHA6QX343-549300N3A5ATI082JA05"
    assert output_stream[6]["recordStatus"] == "new"
    assert output_stream[6]["recordType"] == "relationship"

    assert output_stream[7]["statementId"] == "69905490-a971-8a35-8c82-0422125b15b5"
    assert output_stream[7]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[7]["statementDate"] == "2024-06-12"
    assert output_stream[7]["recordId"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[7]["recordStatus"] == "updated"
    assert output_stream[7]["recordType"] == "entity"

    assert output_stream[8]["statementId"] == "4b674aca-bbb2-bf1b-f4ad-66f0e08f6d1d"
    assert output_stream[8]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[8]["statementDate"] == "2024-06-12"
    assert output_stream[8]["recordId"] == "XI-LEI-RR-D-5493004MMZ3PHA6QX343-549300N3A5ATI082JA05"
    assert output_stream[8]["recordStatus"] == "new"
    assert output_stream[8]["recordType"] == "relationship"

    assert output_stream[9]["statementId"] == "8ed949a1-432b-0cad-fa28-22027abc3bb7"
    assert output_stream[9]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[9]["statementDate"] == "2024-06-12"
    assert output_stream[9]["recordId"] == "XI-LEI-RR-U-5493004MMZ3PHA6QX343-549300N3A5ATI082JA05"
    assert output_stream[9]["recordStatus"] == "updated"
    assert output_stream[9]["recordType"] == "relationship"

    assert output_stream[10]["statementId"] == "251c4aee-a561-e57c-4fc9-d17abcbedaae"
    assert output_stream[10]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[10]["statementDate"] == "2024-07-07"
    assert output_stream[10]["recordId"] == "XI-LEI-RE-D-5493004MMZ3PHA6QX343"
    assert output_stream[10]["recordStatus"] == "closed"
    assert output_stream[10]["recordType"] == "relationship"

    assert output_stream[11]["statementId"] == "89ce86c1-4fd4-8ba5-8798-00d0b6f1f666"
    assert output_stream[11]["declarationSubject"] == "XI-LEI-5493004MMZ3PHA6QX343"
    assert output_stream[11]["statementDate"] == "2024-07-07"
    assert output_stream[11]["recordId"] == "XI-LEI-RE-U-5493004MMZ3PHA6QX343"
    assert output_stream[11]["recordStatus"] == "closed"
    assert output_stream[11]["recordType"] == "relationship"

@pytest.mark.asyncio
@pytest.mark.order(12)
async def test_multiple_updates3_update(wait_for_es, multiple_updates_data3):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(multiple_updates_data3)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 9

    assert output_stream[0]["statementId"] == "53a2fc6d-936c-8866-ce8a-a1c92f6ffc26"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[0]["statementDate"] == "2023-08-04"
    assert output_stream[0]["recordId"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "c5572a64-2bba-8705-74f0-28188d1ef2f0"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[1]["statementDate"] == "2024-06-06"
    assert output_stream[1]["recordId"] == "XI-LEI-RE-D-549300G0XBXOHX8CPW49"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "a2efd1b2-1411-4770-4b91-225776e6d8e7"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[2]["statementDate"] == "2024-06-06"
    assert output_stream[2]["recordId"] == "XI-LEI-RE-U-549300G0XBXOHX8CPW49"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "1657611c-5f4c-10bf-4ed6-b94d05911474"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[3]["statementDate"] == "2024-06-21"
    assert output_stream[3]["recordId"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[3]["recordStatus"] == "updated"
    assert output_stream[3]["recordType"] == "entity"

    assert output_stream[4]["statementId"] == "ac1aef90-2697-620f-6f1d-9ddbc97aba10"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[4]["statementDate"] == "2024-06-21"
    assert output_stream[4]["recordId"] == "XI-LEI-RR-U-549300G0XBXOHX8CPW49-6ZLKQF7QB6JAEKQS5388"
    assert output_stream[4]["recordStatus"] == "new"
    assert output_stream[4]["recordType"] == "relationship"

    assert output_stream[5]["statementId"] == "75f3f972-b897-834c-555c-d097a2fb4d0d"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[5]["statementDate"] == "2024-07-07"
    assert output_stream[5]["recordId"] == "XI-LEI-RE-D-549300G0XBXOHX8CPW49"
    assert output_stream[5]["recordStatus"] == "updated"
    assert output_stream[5]["recordType"] == "relationship"

    assert output_stream[6]["statementId"] == "fa3bf650-e756-2f00-b46d-36ca9d66db72"
    assert output_stream[6]["declarationSubject"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[6]["statementDate"] == "2024-07-07"
    assert output_stream[6]["recordId"] == "XI-LEI-RE-U-549300G0XBXOHX8CPW49"
    assert output_stream[6]["recordStatus"] == "closed"
    assert output_stream[6]["recordType"] == "relationship"

    assert output_stream[7]["statementId"] == "a741e9aa-8db8-2a77-19bd-245bfe7e1be5"
    assert output_stream[7]["declarationSubject"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[7]["statementDate"] == "2024-07-19"
    assert output_stream[7]["recordId"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[7]["recordStatus"] == "updated"
    assert output_stream[7]["recordType"] == "entity"

    assert output_stream[8]["statementId"] == "4f2487b8-79bb-5735-ab89-73f099e7378f"
    assert output_stream[8]["declarationSubject"] == "XI-LEI-549300G0XBXOHX8CPW49"
    assert output_stream[8]["statementDate"] == "2024-07-19"
    assert output_stream[8]["recordId"] == "XI-LEI-RR-U-549300G0XBXOHX8CPW49-6ZLKQF7QB6JAEKQS5388"
    assert output_stream[8]["recordStatus"] == "updated"
    assert output_stream[8]["recordType"] == "relationship"

@pytest.mark.asyncio
@pytest.mark.order(13)
async def test_multiple_updates4_update(wait_for_es, multiple_updates_data4):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(multiple_updates_data4)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 10

    assert output_stream[0]["statementId"] == "8986764b-77b4-805c-e30b-8ff3f1e64b27"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-5493000VFKRQHSOCQ925"
    assert output_stream[0]["statementDate"] == "2023-08-16"
    assert output_stream[0]["recordId"] == "XI-LEI-5493000VFKRQHSOCQ925"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "4194ddbe-9420-50b7-f6fe-4ddc350f6f1c"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-0JK55UGWSWNF3X7KLQ85"
    assert output_stream[1]["statementDate"] == "2023-08-16"
    assert output_stream[1]["recordId"] == "XI-LEI-0JK55UGWSWNF3X7KLQ85"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "entity"

    assert output_stream[2]["statementId"] == "e8293b6d-2c16-0db6-11cf-e0118b3e8eea"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-5493000VFKRQHSOCQ925"
    assert output_stream[2]["statementDate"] == "2023-08-16"
    assert output_stream[2]["recordId"] == "XI-LEI-RR-O-5493000VFKRQHSOCQ925-0JK55UGWSWNF3X7KLQ85"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "37087fee-5dcd-fde1-6ddb-901f56b01038"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-5493000VFKRQHSOCQ925"
    assert output_stream[3]["statementDate"] == "2024-06-06"
    assert output_stream[3]["recordId"] == "XI-LEI-RE-D-5493000VFKRQHSOCQ925"
    assert output_stream[3]["recordStatus"] == "new"
    assert output_stream[3]["recordType"] == "relationship"

    assert output_stream[4]["statementId"] == "39b2b1fa-26f6-190a-3606-cc4e3360417b"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-5493000VFKRQHSOCQ925"
    assert output_stream[4]["statementDate"] == "2024-06-06"
    assert output_stream[4]["recordId"] == "XI-LEI-RE-U-5493000VFKRQHSOCQ925"
    assert output_stream[4]["recordStatus"] == "new"
    assert output_stream[4]["recordType"] == "relationship"

    assert output_stream[5]["statementId"] == "7ecd3700-7b1c-7185-1de7-9d235479ea46"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-0JK55UGWSWNF3X7KLQ85"
    assert output_stream[5]["statementDate"] == "2024-07-05"
    assert output_stream[5]["recordId"] == "XI-LEI-0JK55UGWSWNF3X7KLQ85"
    assert output_stream[5]["recordStatus"] == "updated"
    assert output_stream[5]["recordType"] == "entity"

    assert output_stream[6]["statementId"] == "15d9471a-4ba5-ccb5-23ba-bd5de1118d0e"
    assert output_stream[6]["declarationSubject"] == "XI-LEI-5493000VFKRQHSOCQ925"
    assert output_stream[6]["statementDate"] == "2024-07-18"
    assert output_stream[6]["recordId"] == "XI-LEI-5493000VFKRQHSOCQ925"
    assert output_stream[6]["recordStatus"] == "updated"
    assert output_stream[6]["recordType"] == "entity"

    assert output_stream[7]["statementId"] == "261b1c76-f907-4e77-8db2-21b587561a44"
    assert output_stream[7]["declarationSubject"] == "XI-LEI-5493000VFKRQHSOCQ925"
    assert output_stream[7]["statementDate"] == "2024-07-18"
    assert output_stream[7]["recordId"] == "XI-LEI-RR-O-5493000VFKRQHSOCQ925-0JK55UGWSWNF3X7KLQ85"
    assert output_stream[7]["recordStatus"] == "updated"
    assert output_stream[7]["recordType"] == "relationship"

    assert output_stream[8]["statementId"] == "dc492693-c343-ef37-3a10-77f14a84e940"
    assert output_stream[8]["declarationSubject"] == "XI-LEI-5493000VFKRQHSOCQ925"
    assert output_stream[8]["statementDate"] == "2024-08-07"
    assert output_stream[8]["recordId"] == "XI-LEI-RE-D-5493000VFKRQHSOCQ925"
    assert output_stream[8]["recordStatus"] == "updated"
    assert output_stream[8]["recordType"] == "relationship"

    assert output_stream[9]["statementId"] == "6fa78d14-b7ec-90e1-27b5-2425bb535870"
    assert output_stream[9]["declarationSubject"] == "XI-LEI-5493000VFKRQHSOCQ925"
    assert output_stream[9]["statementDate"] == "2024-08-07"
    assert output_stream[9]["recordId"] == "XI-LEI-RE-U-5493000VFKRQHSOCQ925"
    assert output_stream[9]["recordStatus"] == "updated"
    assert output_stream[9]["recordType"] == "relationship"

@pytest.mark.asyncio
@pytest.mark.order(14)
async def test_multiple_updates5_update(wait_for_es, multiple_updates_data5):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(multiple_updates_data5)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 10

    assert output_stream[0]["statementId"] == "1fee0a46-ea83-e229-ed82-602a76e84876"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-549300AP7B1RB9FQNB27"
    assert output_stream[0]["statementDate"] == "2024-03-28"
    assert output_stream[0]["recordId"] == "XI-LEI-549300AP7B1RB9FQNB27"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "1ba54dd0-2c6a-1665-6d14-06a3168cbf05"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-549300AP7B1RB9FQNB27"
    assert output_stream[1]["statementDate"] == "2024-03-28"
    assert output_stream[1]["recordId"] == "XI-LEI-RR-O-549300AP7B1RB9FQNB27-5493007VV85UTQ8CV292"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "relationship"

    assert output_stream[2]["statementId"] == "556c0a6d-7a5e-7d91-c9fc-80c065c11f54"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-549300AP7B1RB9FQNB27"
    assert output_stream[2]["statementDate"] == "2024-03-28"
    assert output_stream[2]["recordId"] == "XI-LEI-RR-O-549300AP7B1RB9FQNB27-549300TAT0TRDIMPXP71"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "e293554c-990d-2bf7-30ee-a6f9277b0b35"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-549300AP7B1RB9FQNB27"
    assert output_stream[3]["statementDate"] == "2024-06-06"
    assert output_stream[3]["recordId"] == "XI-LEI-RE-D-549300AP7B1RB9FQNB27"
    assert output_stream[3]["recordStatus"] == "new"
    assert output_stream[3]["recordType"] == "relationship"

    assert output_stream[4]["statementId"] == "331e12dd-f480-2b90-fea5-07c23c1078f0"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-549300AP7B1RB9FQNB27"
    assert output_stream[4]["statementDate"] == "2024-06-06"
    assert output_stream[4]["recordId"] == "XI-LEI-RE-U-549300AP7B1RB9FQNB27"
    assert output_stream[4]["recordStatus"] == "new"
    assert output_stream[4]["recordType"] == "relationship"

    assert output_stream[5]["statementId"] == "235bd30b-a558-2e16-9a5e-1a6f5ae09480"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-549300AP7B1RB9FQNB27"
    assert output_stream[5]["statementDate"] == "2024-06-17"
    assert output_stream[5]["recordId"] == "XI-LEI-549300AP7B1RB9FQNB27"
    assert output_stream[5]["recordStatus"] == "updated"
    assert output_stream[5]["recordType"] == "entity"

    assert output_stream[6]["statementId"] == "9812bb74-8561-d2d0-293f-732a457d0606"
    assert output_stream[6]["declarationSubject"] == "XI-LEI-5493007VV85UTQ8CV292"
    assert output_stream[6]["statementDate"] == "2024-05-30"
    assert output_stream[6]["recordId"] == "XI-LEI-5493007VV85UTQ8CV292"
    assert output_stream[6]["recordStatus"] == "new"
    assert output_stream[6]["recordType"] == "entity"

    assert output_stream[7]["statementId"] == "e7169550-8b70-8833-7f7b-9d6298307361"
    assert output_stream[7]["declarationSubject"] == "XI-LEI-549300AP7B1RB9FQNB27"
    assert output_stream[7]["statementDate"] == "2024-06-17"
    assert output_stream[7]["recordId"] == "XI-LEI-RR-O-549300AP7B1RB9FQNB27-5493007VV85UTQ8CV292"
    assert output_stream[7]["recordStatus"] == "updated"
    assert output_stream[7]["recordType"] == "relationship"

    assert output_stream[8]["statementId"] == "4902d2d0-dcbd-9006-6df5-8f44007ed2a8"
    assert output_stream[8]["declarationSubject"] == "XI-LEI-549300AP7B1RB9FQNB27"
    assert output_stream[8]["statementDate"] == "2024-06-17"
    assert output_stream[8]["recordId"] == "XI-LEI-RR-O-549300AP7B1RB9FQNB27-549300TAT0TRDIMPXP71"
    assert output_stream[8]["recordStatus"] == "updated"
    assert output_stream[8]["recordType"] == "relationship"

    assert output_stream[9]["statementId"] == "4c73ecaf-9a45-b8dd-5eb2-581d5fd5be85"
    assert output_stream[9]["declarationSubject"] == "XI-LEI-5493007VV85UTQ8CV292"
    assert output_stream[9]["statementDate"] == "2024-05-30"
    assert output_stream[9]["recordId"] == "XI-LEI-5493007VV85UTQ8CV292"
    assert output_stream[9]["recordStatus"] == "updated"
    assert output_stream[9]["recordType"] == "entity"

@pytest.mark.asyncio
@pytest.mark.order(15)
async def test_multiple_updates6_update(wait_for_es, multiple_updates_data6):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(multiple_updates_data6)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 9

    assert output_stream[0]["statementId"] == "bb53037b-5264-3972-9c9c-33b968ce1b18"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-ZP5ILWVSYE4LJGMMVD57"
    assert output_stream[0]["statementDate"] == "2023-08-09"
    assert output_stream[0]["recordId"] == "XI-LEI-ZP5ILWVSYE4LJGMMVD57"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "30259cdb-dd5d-a6c4-2f3f-71934cbaf1b8"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-549300Q89L7HJNOFLC15"
    assert output_stream[1]["statementDate"] == "2023-07-31"
    assert output_stream[1]["recordId"] == "XI-LEI-549300Q89L7HJNOFLC15"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "entity"

    assert output_stream[2]["statementId"] == "d4ff836b-d21f-0611-c779-c189bfb37ab3"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-549300Q89L7HJNOFLC15"
    assert output_stream[2]["statementDate"] == "2023-07-31"
    assert output_stream[2]["recordId"] == "XI-LEI-RR-D-549300Q89L7HJNOFLC15-54930002LN93NOB84W02"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "03d4973b-b5d3-28bb-a320-ff799ba76411"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-549300Q89L7HJNOFLC15"
    assert output_stream[3]["statementDate"] == "2023-07-31"
    assert output_stream[3]["recordId"] == "XI-LEI-RR-U-549300Q89L7HJNOFLC15-ZP5ILWVSYE4LJGMMVD57"
    assert output_stream[3]["recordStatus"] == "new"
    assert output_stream[3]["recordType"] == "relationship"

    assert output_stream[4]["statementId"] == "1b99f186-6d34-ffbd-b131-111beee3810e"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-549300Q89L7HJNOFLC15"
    assert output_stream[4]["statementDate"] == "2024-07-03"
    assert output_stream[4]["recordId"] == "XI-LEI-549300Q89L7HJNOFLC15"
    assert output_stream[4]["recordStatus"] == "updated"
    assert output_stream[4]["recordType"] == "entity"

    assert output_stream[5]["statementId"] == "f9d9bff6-0b5f-adbb-f355-6a15cef6b37b"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-549300Q89L7HJNOFLC15"
    assert output_stream[5]["statementDate"] == "2024-07-03"
    assert output_stream[5]["recordId"] == "XI-LEI-RR-D-549300Q89L7HJNOFLC15-54930002LN93NOB84W02"
    assert output_stream[5]["recordStatus"] == "updated"
    assert output_stream[5]["recordType"] == "relationship"

    assert output_stream[6]["statementId"] == "cd55310f-d02d-b11d-8973-e0ad8f859f75"
    assert output_stream[6]["declarationSubject"] == "XI-LEI-549300Q89L7HJNOFLC15"
    assert output_stream[6]["statementDate"] == "2024-07-03"
    assert output_stream[6]["recordId"] == "XI-LEI-RR-U-549300Q89L7HJNOFLC15-ZP5ILWVSYE4LJGMMVD57"
    assert output_stream[6]["recordStatus"] == "updated"
    assert output_stream[6]["recordType"] == "relationship"

    assert output_stream[7]["statementId"] == "dcda549c-9546-c871-514e-4b301652b619"
    assert output_stream[7]["declarationSubject"] == "XI-LEI-549300Q89L7HJNOFLC15"
    assert output_stream[7]["statementDate"] == "2024-07-07"
    assert output_stream[7]["recordId"] == "XI-LEI-RE-D-549300Q89L7HJNOFLC15"
    assert output_stream[7]["recordStatus"] == "new"
    assert output_stream[7]["recordType"] == "relationship"

    assert output_stream[8]["statementId"] == "ffb64c68-ef6a-b854-b698-3e4876c9c519"
    assert output_stream[8]["declarationSubject"] == "XI-LEI-ZP5ILWVSYE4LJGMMVD57"
    assert output_stream[8]["statementDate"] == "2024-07-17"
    assert output_stream[8]["recordId"] == "XI-LEI-ZP5ILWVSYE4LJGMMVD57"
    assert output_stream[8]["recordStatus"] == "updated"
    assert output_stream[8]["recordType"] == "entity"

@pytest.mark.asyncio
@pytest.mark.order(16)
async def test_multiple_updates7_update(wait_for_es, multiple_updates_data7):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(multiple_updates_data7)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 10

    assert output_stream[0]["statementId"] == "297e9e97-c736-6195-cc49-3060ce5877d1"
    assert output_stream[0]["declarationSubject"] == "XI-LEI-54930085R8U8HOG2DK36"
    assert output_stream[0]["statementDate"] == "2023-07-31"
    assert output_stream[0]["recordId"] == "XI-LEI-54930085R8U8HOG2DK36"
    assert output_stream[0]["recordStatus"] == "new"
    assert output_stream[0]["recordType"] == "entity"

    assert output_stream[1]["statementId"] == "0a2737d2-6947-c822-a32b-95ca6ef508bd"
    assert output_stream[1]["declarationSubject"] == "XI-LEI-YF0Y5B0IB8SM0ZFG9G81"
    assert output_stream[1]["statementDate"] == "2024-01-26"
    assert output_stream[1]["recordId"] == "XI-LEI-YF0Y5B0IB8SM0ZFG9G81"
    assert output_stream[1]["recordStatus"] == "new"
    assert output_stream[1]["recordType"] == "entity"

    assert output_stream[2]["statementId"] == "c96e679e-b8c6-c3b3-474e-f00cd0a0f54c"
    assert output_stream[2]["declarationSubject"] == "XI-LEI-54930085R8U8HOG2DK36"
    assert output_stream[2]["statementDate"] == "2023-07-31"
    assert output_stream[2]["recordId"] == "XI-LEI-RR-D-54930085R8U8HOG2DK36-YF0Y5B0IB8SM0ZFG9G81"
    assert output_stream[2]["recordStatus"] == "new"
    assert output_stream[2]["recordType"] == "relationship"

    assert output_stream[3]["statementId"] == "10f05240-1797-0283-3dde-784be8ebe06e"
    assert output_stream[3]["declarationSubject"] == "XI-LEI-54930085R8U8HOG2DK36"
    assert output_stream[3]["statementDate"] == "2023-07-31"
    assert output_stream[3]["recordId"] == "XI-LEI-RR-U-54930085R8U8HOG2DK36-YF0Y5B0IB8SM0ZFG9G81"
    assert output_stream[3]["recordStatus"] == "new"
    assert output_stream[3]["recordType"] == "relationship"

    assert output_stream[4]["statementId"] == "1188beb3-5d6d-cc81-d3de-689f9231ee93"
    assert output_stream[4]["declarationSubject"] == "XI-LEI-54930085R8U8HOG2DK36"
    assert output_stream[4]["statementDate"] == "2024-06-21"
    assert output_stream[4]["recordId"] == "XI-LEI-54930085R8U8HOG2DK36"
    assert output_stream[4]["recordStatus"] == "updated"
    assert output_stream[4]["recordType"] == "entity"

    assert output_stream[5]["statementId"] == "7fc9f8ac-a848-b2d4-1481-ebe76d8962ad"
    assert output_stream[5]["declarationSubject"] == "XI-LEI-54930085R8U8HOG2DK36"
    assert output_stream[5]["statementDate"] == "2024-06-21"
    assert output_stream[5]["recordId"] == "XI-LEI-RR-D-54930085R8U8HOG2DK36-YF0Y5B0IB8SM0ZFG9G81"
    assert output_stream[5]["recordStatus"] == "updated"
    assert output_stream[5]["recordType"] == "relationship"

    assert output_stream[6]["statementId"] == "3c3bb690-4f5f-bb1b-402d-90cd850f12bc"
    assert output_stream[6]["declarationSubject"] == "XI-LEI-54930085R8U8HOG2DK36"
    assert output_stream[6]["statementDate"] == "2024-06-21"
    assert output_stream[6]["recordId"] == "XI-LEI-RR-U-54930085R8U8HOG2DK36-YF0Y5B0IB8SM0ZFG9G81"
    assert output_stream[6]["recordStatus"] == "updated"
    assert output_stream[6]["recordType"] == "relationship"

    assert output_stream[7]["statementId"] == "1700eb92-240b-89c5-5009-da6a9d70147e"
    assert output_stream[7]["declarationSubject"] == "XI-LEI-54930085R8U8HOG2DK36"
    assert output_stream[7]["statementDate"] == "2024-07-07"
    assert output_stream[7]["recordId"] == "XI-LEI-RE-U-54930085R8U8HOG2DK36"
    assert output_stream[7]["recordStatus"] == "new"
    assert output_stream[7]["recordType"] == "relationship"

    assert output_stream[8]["statementId"] == "e98c27b0-d386-ecab-d541-52a6bfb361f8"
    assert output_stream[8]["declarationSubject"] == "XI-LEI-54930085R8U8HOG2DK36"
    assert output_stream[8]["statementDate"] == "2024-07-07"
    assert output_stream[8]["recordId"] == "XI-LEI-RE-D-54930085R8U8HOG2DK36"
    assert output_stream[8]["recordStatus"] == "new"
    assert output_stream[8]["recordType"] == "relationship"

    assert output_stream[9]["statementId"] == "f5d61fa4-6646-8387-68fa-78b92431efe3"
    assert output_stream[9]["declarationSubject"] == "XI-LEI-YF0Y5B0IB8SM0ZFG9G81"
    assert output_stream[9]["statementDate"] == "2024-01-26"
    assert output_stream[9]["recordId"] == "XI-LEI-YF0Y5B0IB8SM0ZFG9G81"
    assert output_stream[9]["recordStatus"] == "updated"
    assert output_stream[9]["recordType"] == "entity"

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, lei_json_data_gleif_updated):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(lei_json_data_gleif_updated)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 4


#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, repex_json_data_gleif_deletion):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(repex_json_data_gleif_deletion)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 4

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, repex_json_data_gleif_replaced):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(repex_json_data_gleif_replaced)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, lei_json_data_gleif_issued):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(lei_json_data_gleif_issued)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

# Test data broken
#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, lei_json_data_gleif_retired_gc):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(lei_json_data_gleif_retired_gc)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, lei_json_data_gleif_retired):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(lei_json_data_gleif_retired)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, rr_json_data_gleif_deletion):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(rr_json_data_gleif_deletion)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, rr_json_data_gleif_replaced):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(rr_json_data_gleif_replaced)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, rr_json_data_gleif_updated):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(rr_json_data_gleif_updated)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, repex_json_data_gleif_issued):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(repex_json_data_gleif_issued)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0
