"""
Generate a bcrypt password hash for use in Streamlit Cloud secrets.

Run locally — never on the server, never commit the output to git.

Usage:
    pip install bcrypt
    python generate_password_hash.py

The script prompts twice for the password (hidden), then prints a TOML
snippet you can paste into Streamlit Cloud's Secrets UI.
"""
import getpass
import sys

try:
    import bcrypt
except ImportError:
    print("bcrypt is not installed. Run:  pip install bcrypt", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    print("Generate a bcrypt password hash for a Streamlit Cloud user.")
    print("(Passwords are never stored or printed — only the resulting hash.)")
    print()
    username = input("Username: ").strip()
    if not username:
        print("Username cannot be empty.", file=sys.stderr)
        sys.exit(1)

    role = input("Role [viewer/operator/admin] (default: viewer): ").strip() or "viewer"
    if role not in ("viewer", "operator", "admin"):
        print(f"Invalid role: {role!r}", file=sys.stderr)
        sys.exit(1)

    pw = getpass.getpass("Password: ")
    pw2 = getpass.getpass("Confirm password: ")
    if pw != pw2:
        print("Passwords do not match.", file=sys.stderr)
        sys.exit(1)
    if len(pw) < 8:
        print("Password must be at least 8 characters.", file=sys.stderr)
        sys.exit(1)

    hash_bytes = bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt(rounds=12))
    hash_str = hash_bytes.decode("ascii")

    print()
    print("=" * 64)
    print("Paste this block into Streamlit Cloud Secrets:")
    print("=" * 64)
    print()
    print(f'[auth.users.{username}]')
    print(f'password_hash = "{hash_str}"')
    print(f'role = "{role}"')
    print()
    print("(If you already have [auth.users.*] entries, just add this block.)")


if __name__ == "__main__":
    main()
