"""Pocket Option demo connector using the current unofficial pocket-option SDK.

This module is deliberately isolated behind TradeExecutor. It will refuse to
start unless TRADING_MODE=demo.
"""
import os
import asyncio
from .models import TradeRequest, TradeResult
from .executor import TradeExecutor

class PocketOptionDemoExecutor(TradeExecutor):
    def __init__(self):
        self.ssid = os.getenv("POCKET_OPTION_SSID")
        self.uid = os.getenv("POCKET_OPTION_UID")
        self.platform = os.getenv("POCKET_OPTION_PLATFORM", "1")
        self.client = None
        self.deals_storage = None
        # Track pending close-deal listeners so reconnect() can re-register them
        self._pending_close_listeners = []

    async def connect(self):
        await self._try_connect()
    
    async def _try_connect(self, force_fresh=False):
        if force_fresh or not self.ssid:
            print("Fetching fresh SSID via automated login...")
            from .session_manager import get_fresh_ssid
            self.ssid = await get_fresh_ssid()
            self.uid = os.environ.get("POCKET_OPTION_UID", self.uid)

        if not self.ssid:
            raise RuntimeError(
                "POCKET_OPTION_SSID is missing and automated login failed. "
                "Ensure your email/password are in .env."
            )

        # The SDK is intentionally imported lazily so the rest of the project
        # can still be tested without a broker session.
        from pocket_option import PocketOptionClient
        from pocket_option.models import AuthorizationData
        from pocket_option.constants import Regions
        from pocket_option.contrib.deals import MemoryDealsStorage
        
        # SDK APIs can change because this is unofficial. Keep this code isolated.
        import logging
        self.client = PocketOptionClient(
            logger=True,
            socketio_logger=True,
            engineio_logger=True,
        )
        auth_data = AuthorizationData(
            session=self.ssid,
            uid=int(self.uid) if self.uid else 0,
            isDemo=(os.getenv("TRADING_MODE", "demo").lower() == "demo"),
            isFastHistory=True,
            isOptimized=True,
            platform=int(self.platform) if self.platform else 2,
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
            "Origin": "https://pocketoption.com"
        }
        trading_mode = os.getenv("TRADING_MODE", "demo").lower()
        ws_url = Regions.EUROPA.value if trading_mode == "live" else Regions.DEMO.value
        
        await self.client.connect(
            url=ws_url, 
            auth=None, 
            headers=headers
        )
        
        # Pocket Option backend changed recently: they no longer accept authentication 
        # inside the Socket.IO connect packet (packet 0). They expect it as a standard 
        # event message (packet 42["auth", {...}]).
        
        # We also need to listen for data events because the server might not send "successauth" anymore
        async def on_auth_success(*args):
            self.client.authorized_event.set()
        self.client.add_on("auth/success", on_auth_success)
        self.client.add_on("user_ready", on_auth_success)
        
        # MemoryDealsStorage expects authorization_data to be populated
        self.client.authorization_data = auth_data
        
        # Send the standard SDK auth payload. Note: The SDK serializes this properly.
        await self.client.send("auth", auth_data)
        
        # Wait for the server to confirm authorization before accepting trades
        print("Waiting for broker authorization...")
        authorized = False
        for _ in range(30):
            if self.client.authorized_event.is_set():
                authorized = True
                break
            if not self.client.sio.connected:
                print("[WARNING] Socket disconnected while waiting for authorization.")
                break
            await asyncio.sleep(0.5)
            
        if authorized:
            print("[SUCCESS] Broker authorized and ready!")
        else:
            # If this was a saved SSID, it's probably expired - try a fresh one
            if not force_fresh:
                print("[WARNING] Saved SSID expired or auth failed. Fetching a fresh one...")
                try:
                    await self.client.disconnect()
                except Exception:
                    pass
                self.client = None
                self.ssid = None
                return await self._try_connect(force_fresh=True)
            else:
                print("[WARNING] Authorization failed - continuing anyway, trades may fail.")
        
        self.deals_storage = MemoryDealsStorage(self.client)

        # Re-register any pending close-deal listeners that were set up before
        # a disconnect interrupted get_trade_result().
        self._reattach_close_listeners()

    async def reconnect(self):
        """Lightweight reconnect that preserves deals_storage state.
        
        Unlike connect(), this does NOT destroy the in-memory deal cache.
        After reconnecting, it re-registers any pending close-deal listeners
        and waits for the sync events (updateClosedDeals etc.) to arrive.
        """
        from pocket_option import PocketOptionClient
        from pocket_option.models import AuthorizationData
        from pocket_option.constants import Regions
        from pocket_option.contrib.deals import MemoryDealsStorage

        old_storage = self.deals_storage

        # Gracefully disconnect the old socket if still alive
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass

        # Build a fresh client
        self.client = PocketOptionClient(
            logger=True,
            socketio_logger=True,
            engineio_logger=True,
        )
        auth_data = AuthorizationData(
            session=self.ssid,
            uid=int(self.uid) if self.uid else 0,
            isDemo=(os.getenv("TRADING_MODE", "demo").lower() == "demo"),
            isFastHistory=True,
            isOptimized=True,
            platform=int(self.platform) if self.platform else 2,
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
            "Origin": "https://pocketoption.com"
        }
        trading_mode = os.getenv("TRADING_MODE", "demo").lower()
        ws_url = Regions.EUROPA.value if trading_mode == "live" else Regions.DEMO.value

        await self.client.connect(
            url=ws_url,
            auth=None,
            headers=headers
        )

        async def on_auth_success(*args):
            self.client.authorized_event.set()
        self.client.add_on("auth/success", on_auth_success)
        self.client.add_on("user_ready", on_auth_success)

        self.client.authorization_data = auth_data
        await self.client.send("auth", auth_data)

        print("[RECONNECT] Waiting for broker authorization...")
        authorized = False
        for _ in range(30):
            if self.client.authorized_event.is_set():
                authorized = True
                break
            if not self.client.sio.connected:
                print("[RECONNECT] Socket disconnected while waiting for authorization.")
                break
            await asyncio.sleep(0.5)

        if authorized:
            print("[RECONNECT] Broker authorized and ready!")
        else:
            print("[RECONNECT] Authorization failed after reconnect.")

        # Reattach deals storage to the NEW client so it receives new events,
        # but preserve the old deal cache from the previous connection.
        new_storage = MemoryDealsStorage(self.client)

        # Merge old deals into the new storage so we don't lose history
        if old_storage and hasattr(old_storage, '_deals'):
            if isinstance(old_storage._deals, dict):
                for deal_id, deal in old_storage._deals.items():
                    new_storage._deals[deal_id] = deal
            elif isinstance(old_storage._deals, list):
                existing_ids = {getattr(d, 'id', None) for d in getattr(new_storage, '_deals', [])}
                for deal in old_storage._deals:
                    if getattr(deal, 'id', None) not in existing_ids:
                        new_storage._deals.append(deal)
        
        self.deals_storage = new_storage

        # Re-register any pending close-deal listeners on the new client
        self._reattach_close_listeners()

        # Give time for updateClosedDeals sync events to arrive
        await asyncio.sleep(5)

    def _reattach_close_listeners(self):
        """Re-register all pending close-deal event listeners on the current client."""
        for listener_info in self._pending_close_listeners:
            callback = listener_info['callback']
            try:
                unsub = self.client.on.success_close_deal(callback)
                listener_info['unsub'] = unsub
                print(f"[RECONNECT] Re-registered close-deal listener for {listener_info.get('trade_id', '?')}")
            except Exception as e:
                print(f"[RECONNECT] Failed to re-register close-deal listener: {e}")

    def _resolve_asset(self, asset_str: str):
        """Map a signal asset string like 'USDCHF-OTC' to the SDK's Asset enum."""
        from pocket_option.models import Asset
        
        # Normalize: "USDCHF-OTC" → "USDCHF_otc", "EURUSD" → "EURUSD"
        normalized = asset_str.replace("-OTC", "_otc").replace("-otc", "_otc").replace("_OTC", "_otc").replace(" ", "_").replace("/", "")
        
        # The SDK's Asset enum has a dynamic _missing_ method, meaning we can 
        # pass ANY string to it and it will create a valid Asset for the API.
        # This prevents the bot from accidentally trading the OTC chart when the 
        # signal meant the regular chart.
        try:
            return Asset(normalized)
        except Exception:
            return None

    async def place_trade(self, request: TradeRequest) -> TradeResult:
        if self.client is None or not self.client.sio.connected:
            print("[INFO] Broker socket disconnected. Reconnecting...")
            await self.connect()

        from pocket_option.models import DealAction
        
        asset = self._resolve_asset(request.asset)
        if asset is None:
            return TradeResult(
                accepted=False,
                status="UNSUPPORTED_ASSET",
                message=f"Asset '{request.asset}' not found in SDK. No order was sent.",
            )

        action = DealAction.CALL if request.direction.value == "UP" else DealAction.PUT

        try:
            deal = await self.deals_storage.open_deal(
                asset=asset,
                amount=int(request.amount),
                action=action,
                time=request.expiry_seconds,
            )
            return TradeResult(
                accepted=True,
                trade_id=str(deal.id),
                status="OPEN",
                message=f"Deal opened: {deal.asset} {action.value} ${int(request.amount)} for {request.expiry_seconds}s",
            )
        except Exception as exc:
            return TradeResult(
                accepted=False,
                status="REJECTED",
                message=f"Pocket Option demo request failed: {type(exc).__name__}: {exc}",
            )

    async def get_trade_result(self, trade_id: str, timeout: int = 600) -> TradeResult:
        if self.client is None:
            await self.connect()
        
        if self.deals_storage is None:
            return TradeResult(
                accepted=False,
                trade_id=trade_id,
                status="NOT_CONNECTED",
                message="Deals storage not initialized.",
            )
        
        import uuid
        deal_uuid = uuid.UUID(trade_id)
        
        def _make_result(deal):
            expected_profit = getattr(deal, 'profit', None)
            
            status = "UNKNOWN"
            
            # Primary method: use the profit field directly
            if expected_profit is not None:
                try:
                    p = float(expected_profit)
                    if p < 0:
                        status = "LOSS"
                    elif p > 0:
                        status = "WIN"
                    else:
                        status = "TIE"
                except (ValueError, TypeError):
                    pass
            
            # Fallback method: use open_price and close_price if profit was inconclusive
            if status == "UNKNOWN" and hasattr(deal, 'open_price') and hasattr(deal, 'close_price') and getattr(deal, 'close_price') is not None and getattr(deal, 'close_price') != 0.0:
                if hasattr(deal.command, 'name'):
                    command_str = str(deal.command.name).lower()
                else:
                    command_str = str(deal.command).lower()
                
                if command_str == "call":
                    if deal.close_price > deal.open_price:
                        status = "WIN"
                    elif deal.close_price < deal.open_price:
                        status = "LOSS"
                    else:
                        status = "TIE"
                elif command_str == "put":
                    if deal.close_price < deal.open_price:
                        status = "WIN"
                    elif deal.close_price > deal.open_price:
                        status = "LOSS"
                    else:
                        status = "TIE"
            
            realized_profit = expected_profit if status == "WIN" else (expected_profit if expected_profit and float(expected_profit) < 0 else 0.0)
            
            print(f"[TRADE-RESULT] Deal {trade_id} closed: status={status}, expected_profit={expected_profit}, open={getattr(deal, 'open_price', None)}, close={getattr(deal, 'close_price', None)}")
            return TradeResult(
                accepted=True,
                trade_id=trade_id,
                status=status,
                result=str(deal),
                pnl=float(realized_profit) if realized_profit is not None else None,
            )
        
        # Register a custom listener to capture the raw SuccessCloseDealEvent.
        # The broker sends the ACTUAL realized profit for the deal at the top level of this event,
        # which avoids the issue of the Deal object containing `close_price=0.0`.
        actual_profit = None
        custom_close_event = asyncio.Event()
        
        def on_close_deal(event):
            print(f"[TRADE-RESULT-DEBUG] Received successcloseOrder event. Profit: {event.profit}. Deals in event: {len(event.deals)}")
            for closed_deal in event.deals:
                print(f"[TRADE-RESULT-DEBUG] Checking closed deal ID: {closed_deal.id} against target: {deal_uuid}")
                if closed_deal.id == deal_uuid:
                    nonlocal actual_profit
                    actual_profit = event.profit
                    custom_close_event.set()
        
        # Subscribe to the event and track it for reconnect re-registration
        unsub = self.client.on.success_close_deal(on_close_deal)

        listener_info = {
            'callback': on_close_deal,
            'unsub': unsub,
            'trade_id': trade_id,
        }
        self._pending_close_listeners.append(listener_info)

        reconnect_count = 0
        
        # Poll loop: wait for the close_event from the websocket.
        elapsed = 0
        poll_interval = 2
        last_fallback_check = -999
        while elapsed < timeout:
            if custom_close_event.is_set():
                break
                
            # If socket drops, use reconnect() to preserve state and re-register listeners
            if self.client and not getattr(self.client.sio, 'connected', True):
                reconnect_count += 1
                print(f"[TRADE-RESULT] Socket disconnected for {trade_id}. Attempting reconnect #{reconnect_count}...")
                try:
                    await self.reconnect()
                    # reconnect() already waits 5s for sync events
                    elapsed += 5

                    # After reconnect, immediately check if the deal closed while we were disconnected
                    deal = await self.deals_storage.get_deal(deal_id=deal_uuid)
                    if deal and getattr(deal, 'close_price', 0.0) not in (0.0, None):
                        print(f"[TRADE-RESULT] Deal {trade_id} found closed after reconnect sync!")
                        self._cleanup_listener(listener_info)
                        return _make_result(deal)
                except Exception as e:
                    print(f"[TRADE-RESULT] Reconnect failed: {e}")
            
            # Rate-limit fallback history requests to prevent server spam (check every 15s)
            if elapsed - last_fallback_check >= 15:
                last_fallback_check = elapsed
                # Check deals_storage fallback, but only consider it closed if we have a real close_price
                # deal.closed is True even for open deals because close_timestamp is pre-populated!
                deal = await self.deals_storage.get_deal(deal_id=deal_uuid)
                if deal and getattr(deal, 'close_price', 0.0) not in (0.0, None):
                    print(f"[TRADE-RESULT] Deal {trade_id} found fully closed in deals_storage fallback.")
                    self._cleanup_listener(listener_info)
                    return _make_result(deal)

            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        
        # Clean up the listener tracking
        self._cleanup_listener(listener_info)
            
        if custom_close_event.is_set():
            deal = await self.deals_storage.get_deal(deal_id=deal_uuid)
            
            status = "UNKNOWN"
            if actual_profit is not None:
                if float(actual_profit) <= 0.0:
                    status = "LOSS"
                elif float(actual_profit) > 0.0:
                    status = "WIN"
            elif deal:
                return _make_result(deal)
            
            if status == "LOSS":
                pnl = -float(deal.amount)
            elif status == "WIN":
                pnl = float(getattr(deal, 'profit', 0.0))
            else:
                pnl = 0.0
                
            print(f"[TRADE-RESULT] Deal {trade_id} closed: status={status}, actual_event_profit={actual_profit}, expected_profit={getattr(deal, 'profit', None)}")
            return TradeResult(
                accepted=True,
                trade_id=trade_id,
                status=status,
                result=str(deal),
                pnl=pnl,
            )
        
        # ── Last-resort recovery: reconnect up to 3 more times and check ──
        # This handles the scenario where the socket died right as the deal closed,
        # and the sync events were missed even after the in-loop reconnect.
        print(f"[TRADE-RESULT] Primary poll timed out for {trade_id}. Starting last-resort recovery...")
        for attempt in range(1, 4):
            print(f"[TRADE-RESULT] Last-resort attempt {attempt}/3: reconnecting...")
            try:
                await self.reconnect()
                # Wait extra time for updateClosedDeals to arrive
                await asyncio.sleep(8)

                deal = await self.deals_storage.get_deal(deal_id=deal_uuid)
                if deal and getattr(deal, 'close_price', 0.0) not in (0.0, None):
                    print(f"[TRADE-RESULT] Deal {trade_id} found closed on last-resort attempt {attempt}!")
                    return _make_result(deal)
            except Exception as e:
                print(f"[TRADE-RESULT] Last-resort reconnect attempt {attempt} failed: {e}")
            
            await asyncio.sleep(3)
        
        # Absolute final check on deals_storage
        deal = await self.deals_storage.get_deal(deal_id=deal_uuid)
        if deal and getattr(deal, 'close_price', 0.0) not in (0.0, None):
            print(f"[TRADE-RESULT] Deal {trade_id} found closed after all recovery attempts.")
            return _make_result(deal)
            
        print(f"[TRADE-RESULT] WARNING: Could not determine result for {trade_id} (elapsed={elapsed}s, reconnects={reconnect_count}). Returning UNKNOWN.")
        return TradeResult(
            accepted=True,
            trade_id=trade_id,
            status="UNKNOWN",
            message=f"Result unknown (timeout or network error after {reconnect_count} reconnects).",
        )

    def _cleanup_listener(self, listener_info):
        """Unsubscribe a close-deal listener and remove it from the pending list."""
        try:
            unsub = listener_info.get('unsub')
            if unsub:
                unsub()
        except Exception:
            pass
        if listener_info in self._pending_close_listeners:
            self._pending_close_listeners.remove(listener_info)
