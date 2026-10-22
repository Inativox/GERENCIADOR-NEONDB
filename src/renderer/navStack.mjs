// Pilha isométrica da barra lateral: uma placa por seção visível, na ordem da lista (a primeira fica no topo).
// A placa da aba ativa sobe acima da pilha e acende, deixando vazio o lugar de onde saiu; passar o mouse
// num item adianta a placa dele.
// É só decoração (aria-hidden): a navegação continua nos links, e a pilha apenas observa as classes e o
// display que o renderer já aplica, sem depender da lógica de troca de abas ou de perfil.
export function setupNavStack(root = document) {
    const world = root.querySelector('.nav-stack-world');
    const links = [...root.querySelectorAll('.sidebar-nav .tab-button')];
    if (!world || !links.length) return () => {};
    const plates = new Map(links.map(link => [link, createPlate(root, link, world)]));
    const sync = () => {
        const visible = links.filter(link => link.parentElement.style.display !== 'none');
        world.style.setProperty('--plates', String(visible.length));
        for (const [link, { plate, slot }] of plates) {
            const index = visible.indexOf(link), active = link.classList.contains('active');
            for (const volume of [plate, slot]) {
                volume.hidden = index === -1;
                volume.style.setProperty('--level', String(visible.length - 1 - index));
            }
            plate.classList.toggle('is-active', active);
            // O lugar de onde a placa ativa saiu fica marcado em indigo translúcido.
            slot.classList.toggle('is-empty', active);
        }
    };
    const observer = new MutationObserver(sync);
    for (const link of links) {
        observer.observe(link, { attributes: true, attributeFilter: ['class'] });
        observer.observe(link.parentElement, { attributes: true, attributeFilter: ['style'] });
    }
    sync();
    return () => observer.disconnect();
}

function createPlate(root, link, world) {
    const plate = createVolume(root, 'nav-plate'), slot = createVolume(root, 'nav-plate nav-plate--slot');
    // Atributo próprio: um data-tab-name aqui colidiria com os seletores [data-tab-name] que apontam para os links.
    plate.dataset.plate = link.dataset.tabName;
    const icon = link.querySelector('svg')?.cloneNode(true);
    if (icon) plate.firstElementChild.append(icon);
    const preview = on => () => plate.classList.toggle('is-preview', on);
    link.addEventListener('mouseenter', preview(true));
    link.addEventListener('mouseleave', preview(false));
    link.addEventListener('focus', preview(true));
    link.addEventListener('blur', preview(false));
    world.append(slot, plate);
    return { plate, slot };
}

function createVolume(root, className) {
    const volume = root.createElement('div');
    volume.className = className;
    for (const face of ['top', 'front', 'side']) {
        const element = root.createElement('span');
        element.className = `nav-plate-${face}`;
        volume.append(element);
    }
    return volume;
}
