Name:             dci-analytics
Version:          0.1.0
Release:          1.VERS%{?dist}
Summary:          DCI Analytics engine
License:          ASL 2.0
URL:              https://github.com/redhat-cip/dci-analytics
BuildArch:        noarch
Source0:          dci-analytics-%{version}.tar.gz

BuildRequires:    systemd

%{?systemd_requires}

Requires:         podman

%description
The DCI analytics engine

%prep
%setup -qc

%build

%install
install -p -D -m 644 systemd/%{name}.service %{buildroot}%{_unitdir}/%{name}.service


%post
%systemd_post %{name}.service

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun

%files
%{_unitdir}/*


%changelog
* Wed Oct 20 2021 Yassine Lamgarchal <ylamgarc@redhat.com> - 0.1.0-1
- Initial release
