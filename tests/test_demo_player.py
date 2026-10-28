import unittest
from unittest.mock import MagicMock, patch

import test_core


class DemoPlayerTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_player_and_video_require_login(self):
        for url in ("/demo/", "/demo/heatwave-audit-oci-streaming-demo.mp4"):
            self.assertEqual(self.client.get(url).status_code, 302)

    def test_signed_in_player_uses_external_assets_and_has_no_mp4_download_link(self):
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            response = self.client.get("/demo/")
        self.assertEqual(response.status_code, 200)
        text = response.get_data(as_text=True)
        self.assertIn("controls playsinline", text)
        self.assertIn('src="player.js"', text)
        self.assertIn('href="player.css"', text)
        self.assertNotIn("<style>", text)
        self.assertNotIn("<script>", text)
        self.assertNotIn("Download MP4", text)
        self.assertNotIn("attachment", response.headers.get("Content-Disposition", ""))
        self.assertIn("script-src 'self'", response.headers["Content-Security-Policy"])
        response.close()

    def test_video_supports_byte_ranges_for_browser_playback_and_seeking(self):
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            response = self.client.get(
                "/demo/heatwave-audit-oci-streaming-demo.mp4", headers={"Range": "bytes=0-63"}
            )
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.mimetype, "video/mp4")
        self.assertEqual(len(response.data), 64)
        self.assertTrue(response.headers["Content-Range"].startswith("bytes 0-63/"))
        self.assertTrue(response.headers["Content-Disposition"].startswith("inline;"))
        response.close()

    def test_only_playback_and_companion_assets_are_served(self):
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            for url in ("/demo/source/preview_app.py", "/demo/../app.py", "/demo/storyboard.json"):
                self.assertEqual(self.client.get(url).status_code, 404)
            for name, content_type in [
                ("player.css", "text/css"),
                ("player.js", "text/javascript"),
                ("captions.vtt", "text/vtt"),
            ]:
                response = self.client.get("/demo/" + name)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.mimetype, content_type)
                response.close()

    def test_manual_and_materials_remain_authenticated_and_use_external_assets(self):
        manual = "/docs/user-manual/html/index.html"
        self.assertEqual(self.client.get(manual).status_code, 302)
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            response = self.client.get(manual)
            self.assertEqual(response.status_code, 200)
            text = response.get_data(as_text=True)
            self.assertIn('href="manual.css"', text)
            self.assertIn('src="manual.js"', text)
            self.assertNotIn("<style>", text)
            self.assertNotIn("<script>", text)
            self.assertNotIn("onclick=", text)
            response.close()
            for url in ("/docs/../app.py", "/docs/user-manual/manual-content.json"):
                self.assertEqual(self.client.get(url).status_code, 404)
            response = self.client.get(
                "/docs/user-manual/pdf/HeatWave-Audit-Center-User-Manual.pdf",
                headers={"Range": "bytes=0-63"},
            )
            self.assertEqual(response.status_code, 206)
            self.assertEqual(response.mimetype, "application/pdf")
            response.close()

    def test_private_presentation_download_and_legacy_link(self):
        import tempfile
        from pathlib import Path

        name = 'HeatWave-Audit-Center-Native-OCI-Streaming.pptx'
        url = '/internal_docs/presentations/' + name
        self.assertEqual(self.client.get(url).status_code, 302)
        self.auth()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            folder = root / 'internal_docs/presentations'
            folder.mkdir(parents=True)
            (folder / name).write_bytes(b'presentation fixture' * 20)
            with patch.object(self.mod, 'ROOT', root), patch.object(self.mod.CONNECTIONS, 'borrow', return_value=MagicMock()):
                response = self.client.get(url, headers={'Range': 'bytes=0-63'})
                self.assertEqual(response.status_code, 206)
                self.assertEqual(len(response.data), 64)
                response.close()
                legacy = self.client.get('/docs/presentations/' + name)
                self.assertEqual(legacy.status_code, 302)
                self.assertEqual(legacy.location, url)
                self.assertEqual(self.client.get('/internal_docs/presentations/../../app.py').status_code, 404)
                self.assertEqual(self.client.get('/internal_docs/presentations/private.json').status_code, 404)
