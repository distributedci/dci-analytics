Name:             dci-analytics
Version:          0.1.0
Release:          1.VERS%{?dist}
Summary:          DCI Analytics engine
License:          ASL 2.0
URL:              https://github.com/redhat-cip/dci-analytics
BuildArch:        noarch
Source0:          dci-analytics-%{version}.tar.gz

BuildRequires:  python3-devel
BuildRequires:  python3-flask
BuildRequires:  python3-requests
BuildRequires:  systemd

%{?systemd_requires}

Requires:         podman

%description
The DCI analytics engine

%prep
%setup -qc

%build

%install
install -p -D -m 644 systemd/%{name}.service %{buildroot}%{_unitdir}/%{name}.service
install -p -D -m 644 systemd/%{name}-sync.service %{buildroot}%{_unitdir}/%{name}-sync.service
install -p -D -m 644 systemd/%{name}-sync.timer %{buildroot}%{_unitdir}/%{name}-sync.timer


%files
%{_unitdir}/*

%changelog
* Wed Oct 20 2021 Yassine Lamgarchal <ylamgarc@redhat.com> - 0.1.0-1
- Initial release
