import unittest
from unittest.mock import MagicMock, patch

import test_core


class DemoPlayerTests(unittest.TestCase):
    setUp = test_core.WebTests.setUp
    auth = test_core.WebTests.auth

    def test_player_and_video_require_login(self):
        for url in ("/demo/", "/demo/heatwave-audit-oci-streaming-demo.mp4"):
            self.assertEqual(self.client.get(url).status_code, 302)

    def test_signed_in_demo_opens_the_mp4_directly(self):
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            response = self.client.head("/demo/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "video/mp4")
        self.assertTrue(response.headers["Content-Disposition"].startswith("inline;"))
        self.assertGreater(int(response.headers["Content-Length"]), 64)
        response.close()

    def test_video_supports_byte_ranges_for_browser_playback_and_seeking(self):
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            for url in ("/demo/", "/demo/heatwave-audit-oci-streaming-demo.mp4"):
                with self.subTest(url=url):
                    response = self.client.get(url, headers={"Range": "bytes=0-63"})
                    self.assertEqual(response.status_code, 206)
                    self.assertEqual(response.mimetype, "video/mp4")
                    self.assertEqual(len(response.data), 64)
                    self.assertTrue(response.headers["Content-Range"].startswith("bytes 0-63/"))
                    self.assertTrue(response.headers["Content-Disposition"].startswith("inline;"))
                    response.close()

    def test_only_the_finished_video_is_served(self):
        self.auth()
        with patch.object(self.mod.CONNECTIONS, "borrow", return_value=MagicMock()):
            for name in (
                "source/preview_app.py", "../app.py", "storyboard.json", "verification.json",
                "index.html", "player.css", "player.js", "captions.vtt", "captions.srt",
                "narration.txt", "assets/poster.jpg", "sql/demo-source.sql",
            ):
                with self.subTest(name=name):
                    self.assertEqual(self.client.get("/demo/" + name).status_code, 404)

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
