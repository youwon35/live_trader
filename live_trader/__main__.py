import argparse

from live_trader.process_safety import hold_live_trader_instance_lease


def entrypoint() -> int:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--daemon", action="store_true", help="창 없이 지속 감시 런타임을 실행합니다.")
    parser.add_argument("--profiles", default="stock,crypto")
    parser.add_argument("--mode", default="MONITOR")
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    args = parser.parse_args()
    instance = hold_live_trader_instance_lease()
    if instance.get("acquired") is not True:
        print(
            "Live Trader가 이미 실행 중이거나 단일 인스턴스 잠금을 "
            f"획득하지 못했습니다: {instance.get('reason') or 'unknown'}"
        )
        return 2
    if args.daemon:
        from live_trader.daemon import run_daemon

        return run_daemon(args.profiles.split(","), str(args.mode).upper(), args.poll_seconds)
    from live_trader.desktop import main

    main()
    return 0


if __name__ == "__main__":
    raise SystemExit(entrypoint())
