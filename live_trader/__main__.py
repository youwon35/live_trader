import argparse

from live_trader.desktop import main


def entrypoint() -> int:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--daemon", action="store_true", help="창 없이 지속 감시 런타임을 실행합니다.")
    parser.add_argument("--profiles", default="stock,crypto")
    parser.add_argument("--mode", default="MONITOR")
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    args = parser.parse_args()
    if args.daemon:
        from live_trader.daemon import run_daemon

        return run_daemon(args.profiles.split(","), str(args.mode).upper(), args.poll_seconds)
    main()
    return 0


if __name__ == "__main__":
    raise SystemExit(entrypoint())
