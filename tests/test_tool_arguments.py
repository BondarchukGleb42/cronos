from cronos.capabilities import normalize_tool_arguments


def test_optional_empty_ids_do_not_turn_creation_into_an_update():
    args = {"recipe_id": " \n", "name": "Утро", "description": "", "revision": 0}
    assert normalize_tool_arguments("recipe_save", args) == {
        "name": "Утро", "description": "", "revision": 0
    }
    assert args["recipe_id"] == " \n"


def test_required_ids_and_empty_expiry_are_never_silently_removed():
    args = {"memory_id": "", "content": "", "expires_at": ""}
    assert normalize_tool_arguments("memory_update", args) == args
    assert normalize_tool_arguments("memory_write", {"expires_at": ""}) == {"expires_at": ""}


def test_nonempty_or_wrong_type_target_still_reaches_owner_and_uuid_validation():
    for value in ("another-users-id", "  invalid  ", False, 0, []):
        args = {"project_id": value, "scope": "global"}
        assert normalize_tool_arguments("memory_write", args) == args


def test_only_declared_optional_ids_are_normalized():
    assert normalize_tool_arguments("project_get", {"project_id": None}) == {}
    args = {"unknown_id": "", "scope": ""}
    assert normalize_tool_arguments("project_get", args) == args
    assert normalize_tool_arguments("unknown_tool", {"project_id": ""}) == {"project_id": ""}
