################################################################################
# vmbackup - Build, Install & Deploy
#
# Usage:
#   make package              Build .deb package (Debian/Ubuntu)
#   make install              Install from source (any distro)
#   make uninstall            Remove vmbackup (keeps backup data)
#   make deploy TARGET=root@host        SCP + install on target host
#   make clean                Remove build artifacts
#   make version              Show current version
#
# The version is read from VMBACKUP_VERSION in lib/version.sh — bump it there.
# (UNI-008 moved the constant out of vmbackup.sh into the shared lib.)
################################################################################

PKG_NAME    := vmbackup
# UNI-008: Version source moved from vmbackup.sh to lib/version.sh
VERSION     := $(shell grep '^[[:space:]]*readonly[[:space:]]\+VMBACKUP_VERSION=' lib/version.sh | head -1 | sed 's/.*"\(.*\)"/\1/')
ARCH        := all
INSTALL_DIR := /opt/vmbackup
BUILD_DIR   := build
PKG_DIR     := $(BUILD_DIR)/$(PKG_NAME)_$(VERSION)_$(ARCH)
DEB_FILE    := $(PKG_DIR).deb

.PHONY: package clean deploy version install uninstall test lint lint-baseline

version:
	@echo "$(PKG_NAME) $(VERSION)"

# Run all tests under tests/. Each test is a self-contained bash script that
# exits 0 on pass, non-zero on fail. Naming convention: {ticket}-{phase}-{description}.sh
# (e.g. tests/109-phase3-period-vectors.sh). See tests/README.md for details.
test:
	@set -e; \
	if [ ! -d tests ] || [ -z "$$(ls tests/*.sh 2>/dev/null)" ]; then \
		echo "no tests in tests/"; exit 0; \
	fi; \
	pass=0; fail=0; failed=""; \
	for t in tests/*.sh; do \
		printf '== %s ==\n' "$$t"; \
		if bash "$$t"; then pass=$$((pass+1)); else fail=$$((fail+1)); failed="$$failed $$t"; fi; \
	done; \
	echo; echo "tests: $$pass passed, $$fail failed"; \
	if [ $$fail -gt 0 ]; then echo "failed:$$failed"; exit 1; fi

package: clean
	@echo "=== Building $(PKG_NAME) $(VERSION) ==="

	# --- Directory structure ---
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/modules
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/lib
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/transports
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/cloud_transports
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/config/default
	mkdir -p $(PKG_DIR)$(INSTALL_DIR)/config/template
	mkdir -p $(PKG_DIR)/DEBIAN

	# --- Main scripts (750: root + backup group, no world) ---
	install -m 750 vmbackup.sh             $(PKG_DIR)$(INSTALL_DIR)/
	install -m 750 vmrestore.sh            $(PKG_DIR)$(INSTALL_DIR)/

	# --- Modules (640: root rw, backup group read, no world) ---
	install -m 640 modules/*.sh            $(PKG_DIR)$(INSTALL_DIR)/modules/

	# --- Libraries (640: root rw, backup group read, no world) ---
	install -m 640 lib/*.sh                $(PKG_DIR)$(INSTALL_DIR)/lib/

	# --- Transports (750: root + backup group, no world) ---
	install -m 750 transports/*.sh         $(PKG_DIR)$(INSTALL_DIR)/transports/

	# --- Cloud transports (750: root + backup group, no world) ---
	install -m 750 cloud_transports/*.sh   $(PKG_DIR)$(INSTALL_DIR)/cloud_transports/

	# --- Configs: default (640: root rw, backup group read, no world) ---
	install -m 640 config/default/*        $(PKG_DIR)$(INSTALL_DIR)/config/default/

	# --- Configs: template (640: root rw, backup group read, no world) ---
	install -m 640 config/template/*       $(PKG_DIR)$(INSTALL_DIR)/config/template/

	# --- AppArmor snippet ---
	mkdir -p $(PKG_DIR)/etc/apparmor.d/local/abstractions
	install -m 644 apparmor/libvirt-qemu.local $(PKG_DIR)/etc/apparmor.d/local/abstractions/libvirt-qemu

	# --- Documentation ---
	install -m 644 vmbackup.md $(PKG_DIR)$(INSTALL_DIR)/
	install -m 644 vmrestore.md $(PKG_DIR)$(INSTALL_DIR)/

	# --- systemd units (under /usr/lib per modern Debian; /lib is symlink) ---
	install -d -m 755 $(PKG_DIR)/usr/lib/systemd/system
	install -m 644 systemd/vmbackup.service $(PKG_DIR)/usr/lib/systemd/system/
	install -m 644 systemd/vmbackup.timer   $(PKG_DIR)/usr/lib/systemd/system/

	# --- /usr/share/doc/vmbackup: copyright + Debian changelog ---
	install -d -m 755 $(PKG_DIR)/usr/share/doc/$(PKG_NAME)
	install -m 644 debian/copyright $(PKG_DIR)/usr/share/doc/$(PKG_NAME)/copyright
	# Generate proper Debian-format changelog from upstream CHANGELOG.md
	printf '%s (%s) unstable; urgency=medium\n\n  * See the upstream changelog for the full release history:\n    https://github.com/doutsis/vmbackup/blob/main/CHANGELOG.md\n\n -- James Doutsis <james@doutsis.com>  %s\n' \
	  '$(PKG_NAME)' '$(VERSION)' "$$(date -R)" \
	  | gzip -9n > $(PKG_DIR)/usr/share/doc/$(PKG_NAME)/changelog.Debian.gz
	chmod 644 $(PKG_DIR)/usr/share/doc/$(PKG_NAME)/changelog.Debian.gz

	# --- lintian overrides ---
	install -d -m 755 $(PKG_DIR)/usr/share/lintian/overrides
	install -m 644 debian/lintian-overrides $(PKG_DIR)/usr/share/lintian/overrides/$(PKG_NAME)

	# --- Normalise directory permissions to 0755 (umask=002 leaks 0775) ---
	find $(PKG_DIR) -type d ! -path '$(PKG_DIR)/DEBIAN' ! -path '$(PKG_DIR)/DEBIAN/*' -exec chmod 755 {} +

	# --- DEBIAN metadata ---
	sed 's/__VERSION__/$(VERSION)/' debian/control > $(PKG_DIR)/DEBIAN/control
	install -m 644 debian/conffiles        $(PKG_DIR)/DEBIAN/
	install -m 755 debian/postinst         $(PKG_DIR)/DEBIAN/
	install -m 755 debian/postrm           $(PKG_DIR)/DEBIAN/

	# --- Build ---
	dpkg-deb --build --root-owner-group $(PKG_DIR)

	@echo ""
	@echo "=== Package built: $(DEB_FILE) ==="
	@echo "    Size: $$(du -h $(DEB_FILE) | cut -f1)"
	@echo ""
	@echo "To deploy:  make deploy TARGET=root@host"

deploy:
	@test -n "$(TARGET)" || { echo "Usage: make deploy TARGET=user@host"; exit 1; }
	@test -f "$(DEB_FILE)" || { echo "No package found. Run 'make package' first."; exit 1; }
	@echo "=== Deploying $(PKG_NAME) $(VERSION) to $(TARGET) ==="
	scp $(DEB_FILE) $(TARGET):/tmp/$(PKG_NAME)_$(VERSION)_$(ARCH).deb
	ssh $(TARGET) "dpkg -i /tmp/$(PKG_NAME)_$(VERSION)_$(ARCH).deb && rm -f /tmp/$(PKG_NAME)_$(VERSION)_$(ARCH).deb"
	@echo ""
	@echo "=== Deployed $(PKG_NAME) $(VERSION) to $(TARGET) ==="

install:
	@echo "=== Installing $(PKG_NAME) $(VERSION) to $(INSTALL_DIR) ==="
	@test "$$(id -u)" = "0" || { echo "Error: make install must be run as root (use sudo)"; exit 1; }

	# --- Create directory structure ---
	mkdir -p $(INSTALL_DIR)
	mkdir -p $(INSTALL_DIR)/modules
	mkdir -p $(INSTALL_DIR)/lib
	mkdir -p $(INSTALL_DIR)/transports
	mkdir -p $(INSTALL_DIR)/cloud_transports
	mkdir -p $(INSTALL_DIR)/config/template

	# --- Install files with correct permissions ---
	install -m 750 vmbackup.sh             $(INSTALL_DIR)/
	install -m 750 vmrestore.sh            $(INSTALL_DIR)/
	install -m 640 modules/*.sh            $(INSTALL_DIR)/modules/
	install -m 640 lib/*.sh                $(INSTALL_DIR)/lib/
	install -m 750 transports/*.sh         $(INSTALL_DIR)/transports/
	install -m 750 cloud_transports/*.sh   $(INSTALL_DIR)/cloud_transports/
	install -m 640 config/template/*       $(INSTALL_DIR)/config/template/
	install -m 644 vmbackup.md             $(INSTALL_DIR)/
	install -m 644 vmrestore.md            $(INSTALL_DIR)/

	# --- Default config: copy template if no existing config ---
	@if [ ! -d "$(INSTALL_DIR)/config/default" ]; then \
		mkdir -p $(INSTALL_DIR)/config/default; \
		install -m 640 config/default/* $(INSTALL_DIR)/config/default/; \
		echo "Config: installed defaults to $(INSTALL_DIR)/config/default/"; \
	else \
		echo "Config: $(INSTALL_DIR)/config/default/ already exists, not overwritten"; \
	fi

	# --- AppArmor snippet ---
	mkdir -p /etc/apparmor.d/local/abstractions
	install -m 644 apparmor/libvirt-qemu.local /etc/apparmor.d/local/abstractions/libvirt-qemu

	# --- systemd units ---
	install -m 644 systemd/vmbackup.service /lib/systemd/system/
	install -m 644 systemd/vmbackup.timer   /lib/systemd/system/

	# --- PATH symlink ---
	ln -sf $(INSTALL_DIR)/vmbackup.sh /usr/local/bin/vmbackup
	ln -sf $(INSTALL_DIR)/vmrestore.sh /usr/local/bin/vmrestore

	# --- Ensure backup group exists ---
	@if ! getent group backup >/dev/null 2>&1; then \
		groupadd --system backup; \
	fi

	# --- Ownership and permissions ---
	chown -R root:backup $(INSTALL_DIR)
	chmod 750 $(INSTALL_DIR)
	# SEC-02: strip group/other-write recursively (mirror package-target's L107) so a
	# backup-group member can't replace code the root nightly sources; self-heals
	# umask-002 dir drift + world-readable config files. Capital X keeps exec bits.
	chmod -R u=rwX,g=rX,o= $(INSTALL_DIR)
	mkdir -p /var/log/vmbackup /run/vmbackup
	chown root:backup /var/log/vmbackup /run/vmbackup
	chmod 750 /var/log/vmbackup /run/vmbackup

	# --- Reload AppArmor profiles if active ---
	@if command -v aa-status >/dev/null 2>&1 && aa-status --enabled 2>/dev/null; then \
		for profile in /etc/apparmor.d/libvirt/libvirt-*; do \
			case "$$profile" in *.files) continue;; esac; \
			case "$$(basename $$profile)" in TEMPLATE.qemu) continue;; esac; \
			apparmor_parser -r "$$profile" 2>/dev/null || true; \
		done; \
		echo "AppArmor: reloaded libvirt VM profiles"; \
	fi

	# --- systemd ---
	systemctl daemon-reload 2>/dev/null || true
	@if ! systemctl is-enabled vmbackup.timer >/dev/null 2>&1; then \
		systemctl enable vmbackup.timer 2>/dev/null || true; \
	fi

	@echo ""
	@echo "=== $(PKG_NAME) $(VERSION) installed ==="
	@echo ""
	@echo "  Command:       vmbackup --version"
	@echo "  Timer:         sudo systemctl start vmbackup.timer"
	@echo "  Manual run:    sudo vmbackup --run"
	@echo "  Config:        $(INSTALL_DIR)/config/default/"
	@echo ""

clean:
	rm -rf $(BUILD_DIR)

uninstall:
	@echo "=== Uninstalling $(PKG_NAME) from $(INSTALL_DIR) ==="
	@test "$$(id -u)" = "0" || { echo "Error: make uninstall must be run as root (use sudo)"; exit 1; }

	# --- Stop and disable systemd units ---
	systemctl stop vmbackup.timer 2>/dev/null || true
	systemctl stop vmbackup.service 2>/dev/null || true
	systemctl disable vmbackup.timer 2>/dev/null || true

	# --- Remove installed files ---
	rm -f /usr/local/bin/vmbackup
	rm -f /usr/local/bin/vmrestore
	rm -f /lib/systemd/system/vmbackup.service
	rm -f /lib/systemd/system/vmbackup.timer
	rm -f /etc/apparmor.d/local/abstractions/libvirt-qemu
	rm -rf $(INSTALL_DIR)
	rm -rf /var/log/vmbackup
	rm -rf /var/log/vmrestore

	systemctl daemon-reload 2>/dev/null || true

	@echo ""
	@echo "=== $(PKG_NAME) uninstalled ==="
	@echo "Backup data was not touched."
	@echo ""

# ── Lint gate (added 2026-05-06 by Phase 3.5; see copilot/109-phase3.5-spec.md) ──
#
# `make lint` runs shellcheck across every tracked *.sh file, diffs against
# tests/lint-baseline.txt, and fails ONLY on additions. Removals (improvements)
# are silently allowed; pick them up at the next `make lint-baseline`.
#
# `make lint-baseline` regenerates tests/lint-baseline.txt from scratch. Use
# after fixing a real bug surfaced by the gate, or rarely, to absorb a
# warning that has been judged acceptable noise.
#
# The find-pipeline below is the canonical source of truth — keep it in
# sync with copilot/109-phase3.5-spec.md §2 if either ever changes.

lint-baseline:
	@test -d tests || { echo "lint-baseline: tests/ not shipped in this tree (development-repo target) — skipping"; exit 0; }
	@find . -name '*.sh' \
	     -not -path './.git/*' \
	     -not -path './tests/tmp/*' \
	     -not -path './tests/fixtures/*' \
	     -not -path './vmbackup-manager/*' \
	     -not -path './build/*' \
	     -not -path './archive/*' \
	     -not -path './tests/soak/*' \
	  | LC_ALL=C sort \
	  | xargs shellcheck -f gcc 2>&1 \
	  | LC_ALL=C sort -u \
	  > tests/lint-baseline.txt
	@echo "Regenerated tests/lint-baseline.txt ($$(wc -l < tests/lint-baseline.txt) lines)."

lint:
	@test -f tests/lint-baseline.txt || { echo "lint: tests/ not shipped in this tree (development-repo target) — skipping"; exit 0; }
	@actual=$$(mktemp); \
	find . -name '*.sh' \
	     -not -path './.git/*' \
	     -not -path './tests/tmp/*' \
	     -not -path './tests/fixtures/*' \
	     -not -path './vmbackup-manager/*' \
	     -not -path './build/*' \
	     -not -path './archive/*' \
	     -not -path './tests/soak/*' \
	  | LC_ALL=C sort \
	  | xargs shellcheck -f gcc 2>&1 \
	  | LC_ALL=C sort -u \
	  > "$$actual"; \
	added=$$(comm -13 tests/lint-baseline.txt "$$actual"); \
	rm -f "$$actual"; \
	if [ -n "$$added" ]; then \
	  echo "make lint: NEW shellcheck findings not in tests/lint-baseline.txt:"; \
	  echo "$$added"; \
	  echo ""; \
	  echo "Either fix the underlying issue, or (rarely) accept it by"; \
	  echo "running 'make lint-baseline' and committing the new baseline."; \
	  exit 1; \
	fi; \
	echo "make lint: OK (no new findings)."
